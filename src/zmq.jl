module Zmq

import Sockets
import ZMQ

using Peven

using ..IPC
using ..Router

struct ZmqError <: Exception
    message::String
end

Base.showerror(io::IO, error::ZmqError) = print(io, error.message)

struct PendingCall
    identity::Vector{UInt8}
    reply::Channel{Any}
end

struct OutboundSend
    identity::Vector{UInt8}
    payload::Vector{UInt8}
end

mutable struct Gateway
    socket::ZMQ.Socket
    identities::Dict{String,Vector{UInt8}}
    pendingCalls::Dict{Int,PendingCall}
    outboundSends::Channel{OutboundSend}
    nets::Dict{String,Peven.Net}
    activeFires::Set{String}
    lifecycle::Symbol
    lifecycleLock::ReentrantLock
    identityLock::ReentrantLock
    callLock::ReentrantLock
    socketLock::ReentrantLock
    controlLock::ReentrantLock
end

Gateway(socket::ZMQ.Socket) =
    Gateway(
        socket,
        Dict{String,Vector{UInt8}}(),
        Dict{Int,PendingCall}(),
        Channel{OutboundSend}(1024),
        Dict{String,Peven.Net}(),
        Set{String}(),
        :open,
        ReentrantLock(),
        ReentrantLock(),
        ReentrantLock(),
        ReentrantLock(),
        ReentrantLock(),
    )

# raw libzmq option ids; ZMQ.jl wraps none of these (ROUTER_NOTIFY is a draft API,
# verified present in our pinned ZeroMQ_jll 4.3.5 — setLivenessOptions! throws at
# startup if a future libzmq build lacks it)
const zmqHeartbeatIvl = 75
const zmqHeartbeatTtl = 76
const zmqHeartbeatTimeout = 77
const zmqRouterNotify = 97
const zmqNotifyDisconnect = 2

const heartbeatIvlMs = 2000
const heartbeatTimeoutMs = 6000

function setLivenessOptions!(socket::ZMQ.Socket)
    setSocketOption!(socket, zmqHeartbeatIvl, heartbeatIvlMs)
    setSocketOption!(socket, zmqHeartbeatTimeout, heartbeatTimeoutMs)
    setSocketOption!(socket, zmqHeartbeatTtl, heartbeatTimeoutMs)
    setSocketOption!(socket, zmqRouterNotify, zmqNotifyDisconnect)
    return nothing
end

function setSocketOption!(socket::ZMQ.Socket, option::Integer, value::Integer)
    rc = ZMQ.lib.zmq_setsockopt(socket, option, Ref{Cint}(value), sizeof(Cint))
    rc == 0 || throw(ZmqError("failed to set libzmq socket option $(option)"))
    return nothing
end

function gateway(endpoint::String)
    socket = ZMQ.Socket(ZMQ.ROUTER)
    socket.rcvtimeo = 1
    setLivenessOptions!(socket)
    Sockets.bind(socket, endpoint)
    return Gateway(socket)
end

function run!(gateway::Gateway, routerState::Router.RouterState)
    startGateway!(gateway)
    try
        while gatewayRunning(gateway)
            sendOutbound!(gateway)
            try
                dispatch!(gateway, routerState)
            catch error
                error isa ZMQ.TimeoutError || rethrow(error)
                sendOutbound!(gateway)
                sleep(0.001)
            end
        end
        sendOutbound!(gateway)
    finally
        markClosed!(gateway)
        drainOutbound!(gateway)
        failPendingCalls!(gateway, "gateway stopped")
        close(gateway.socket)
    end
    return nothing
end

function stop!(gateway::Gateway)
    lock(gateway.lifecycleLock)
    try
        gateway.lifecycle == :running && (gateway.lifecycle = :stopping)
        gateway.lifecycle == :open && (gateway.lifecycle = :closed)
    finally
        unlock(gateway.lifecycleLock)
    end
    return nothing
end

function dispatch!(gateway::Gateway, routerState::Router.RouterState)
    frames = withSocketLock(gateway) do
        ZMQ.recv_multipart(gateway.socket, Vector{UInt8})
    end
    identity = frames[1]
    if length(frames) != 2
        replyGatewayError!(
            gateway, identity, "expected ROUTER message with identity and payload"
        )
        return nothing
    end
    payload = frames[2]
    isempty(payload) && return disconnectWorker!(gateway, routerState, identity)
    message = IPC.parseWorkerMessage(payload)
    if message isa IPC.Malformed
        replyGatewayError!(gateway, identity, message.reason)
        return nothing
    end
    try
        handle!(gateway, routerState, identity, message)
    catch error
        # parseWorkerMessage already absorbed everything bytes can cause, so
        # this whitelist is exact: protocol-state rejections reply, internal
        # bugs crash loudly.
        if error isa ZmqError || error isa IPC.IpcError || error isa Router.RouterError
            replyGatewayError!(gateway, identity, sprint(showerror, error))
            return nothing
        end
        rethrow(error)
    end
    return nothing
end

# Best-effort: a failed error reply must not take down the dispatch loop.
function replyGatewayError!(gateway::Gateway, identity::Vector{UInt8}, message::String)
    reply = IPC.gatewayError(message) |> IPC.encode
    try
        withSocketLock(gateway) do
            sendWorker!(gateway, identity, reply)
        end
    catch error
        error isa InterruptException && rethrow()
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.ExecutorReply,
)
    completeCall!(gateway, identity, message.callId, message.payload)
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.WorkerHello,
)
    recordIdentity!(gateway, message.workerId, identity)
    Router.registerWorker!(routerState, message.workerId)
    reply = IPC.workerReady(message.workerId) |> IPC.encode
    withSocketLock(gateway) do
        sendWorker!(gateway, identity, reply)
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.WorkerGoodbye,
)
    expected = workerIdentity(gateway, message.workerId)
    expected == identity ||
        throw(ZmqError("workerGoodbye came from unexpected identity"))
    forgetWorker!(gateway, routerState, message.workerId)
    reply = IPC.workerGone(message.workerId) |> IPC.encode
    withSocketLock(gateway) do
        sendWorker!(gateway, identity, reply)
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.Assign,
)
    expected = workerIdentity(gateway, message.workerId)
    expected == identity || throw(ZmqError("assign came from unexpected identity"))
    Router.route!(routerState, message.runKey, message.workerId)
    reply = IPC.assigned(message.runKey, message.workerId) |> IPC.encode
    withSocketLock(gateway) do
        sendWorker!(gateway, identity, reply)
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.Release,
)
    workerId = Router.workerForRun(routerState, message.runKey)
    expected = workerIdentity(gateway, workerId)
    expected == identity || throw(ZmqError("release came from unexpected identity"))
    Router.unroute!(routerState, message.runKey)
    reply = IPC.released(message.runKey) |> IPC.encode
    withSocketLock(gateway) do
        sendWorker!(gateway, identity, reply)
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.LoadNet,
)
    # registerNet! writes the engine's unlocked executor registry, which
    # active fires read concurrently — reject instead of racing.
    lock(gateway.controlLock) do
        isempty(gateway.activeFires) ||
            throw(ZmqError("cannot load a net while fires are active"))
    end
    issues = Peven.validate!(Peven.ValidationIssue[], message.net)
    isempty(issues) ||
        throw(ZmqError("invalid net $(repr(message.name)): $(issues[1].message)"))
    registerNet!(gateway, routerState, message.name, message.net)
    reply = IPC.netLoaded(message.name) |> IPC.encode
    withSocketLock(gateway) do
        sendWorker!(gateway, identity, reply)
    end
    return nothing
end

function handle!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    message::IPC.Fire,
)
    fire!(gateway, routerState, identity, message)
    return nothing
end

function registerNet!(
    gateway::Gateway,
    routerState::Router.RouterState,
    name::String,
    net::Peven.Net,
)
    lock(gateway.controlLock) do
        gateway.nets[name] = net
    end
    for transition in values(net.transitions)
        Peven.registerExec!(
            transition.executor,
            Router.PythonExecutor(transition.executor, routerState, gateway),
        )
    end
    return nothing
end

# Once decodeFire succeeds the fireId is known, so every failure from here on
# is a correlated fireFinished — the control client always gets closure.
function fire!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
    request,
)
    error = claimFire!(gateway, routerState, request)
    if !isnothing(error)
        sendControl!(gateway, identity, IPC.fireFinished(request.fireId, error))
        return nothing
    end
    net = lock(gateway.controlLock) do
        gateway.nets[request.net]
    end
    Threads.@spawn streamFire!(gateway, identity, request, net)
    return nothing
end

# One atomic claim: duplicate fireId, unknown net, and unrouted runKeys all
# reject before the fireId is held.
function claimFire!(gateway::Gateway, routerState::Router.RouterState, request)
    lock(gateway.controlLock) do
        request.fireId in gateway.activeFires &&
            return "fireId $(repr(request.fireId)) is already active"
        haskey(gateway.nets, request.net) ||
            return "unknown net $(repr(request.net))"
        for bucket in values(request.marking.tokensByPlace), token in bucket
            runKey = Peven.runKey(token)
            Router.isRouted(routerState, runKey) ||
                return "no worker assigned for runKey $(repr(runKey))"
        end
        push!(gateway.activeFires, request.fireId)
        return nothing
    end
end

function streamFire!(
    gateway::Gateway,
    identity::Vector{UInt8},
    request,
    net::Peven.Net,
)
    error = try
        Peven.fire(
            net,
            request.marking;
            fuse = request.fuse,
            maxConcurrency = request.maxConcurrency,
            onEvent = event -> streamRunFinished!(gateway, identity, request.fireId, event),
        )
        nothing
    catch caught
        sprint(showerror, caught)
    finally
        lock(gateway.controlLock) do
            delete!(gateway.activeFires, request.fireId)
        end
    end
    sendControl!(gateway, identity, IPC.fireFinished(request.fireId, error))
    return nothing
end

function streamRunFinished!(
    gateway::Gateway,
    identity::Vector{UInt8},
    fireId::String,
    event,
)
    event isa Peven.RunFinished || return nothing
    sendControl!(gateway, identity, IPC.runFinished(fireId, event.result))
    return nothing
end

function sendControl!(gateway::Gateway, identity::Vector{UInt8}, message)
    put!(gateway.outboundSends, OutboundSend(identity, IPC.encode(message)))
    return nothing
end

function Router.callWorker(gateway::Gateway, workerId::String, payload::Vector{UInt8})
    gatewayRunning(gateway) || throw(ZmqError("gateway is not running"))
    callId = IPC.callId(IPC.decode(payload))
    identity = workerIdentity(gateway, workerId)
    channel = registerCall!(gateway, callId, identity)
    try
        put!(gateway.outboundSends, OutboundSend(identity, payload))
        if !hasIdentity(gateway, workerId, identity)
            drainOutbound!(gateway, identity)
            cancelCall!(gateway, callId)
            throw(ZmqError("worker $(repr(workerId)) disconnected before call was sent"))
        end
        if !gatewayRunning(gateway)
            cancelCall!(gateway, callId)
            throw(ZmqError("gateway stopped before call was sent"))
        end
        reply = take!(channel)
        reply isa Exception && throw(reply)
        return reply
    catch error
        cancelCall!(gateway, callId)
        rethrow(error)
    end
end

function hasIdentity(gateway::Gateway, workerId::String, identity::Vector{UInt8})
    lock(gateway.identityLock) do
        get(gateway.identities, workerId, nothing) == identity
    end
end

function startGateway!(gateway::Gateway)
    lock(gateway.lifecycleLock)
    try
        if gateway.lifecycle == :running
            throw(ZmqError("gateway is already running"))
        end
        if gateway.lifecycle == :stopping || gateway.lifecycle == :closed
            throw(ZmqError("gateway is stopped"))
        end
        gateway.lifecycle == :open || throw(ZmqError("unknown gateway lifecycle"))
        gateway.lifecycle = :running
    finally
        unlock(gateway.lifecycleLock)
    end
    return nothing
end

function markClosed!(gateway::Gateway)
    lock(gateway.lifecycleLock)
    try
        gateway.lifecycle = :closed
    finally
        unlock(gateway.lifecycleLock)
    end
    return nothing
end

function gatewayRunning(gateway::Gateway)
    lock(gateway.lifecycleLock) do
        gateway.lifecycle == :running
    end
end

function sendWorker!(gateway::Gateway, identity::Vector{UInt8}, payload::Vector{UInt8})
    ZMQ.send_multipart(gateway.socket, [identity, payload])
    return nothing
end

function sendOutbound!(gateway::Gateway)
    while isready(gateway.outboundSends)
        outbound = take!(gateway.outboundSends)
        try
            withSocketLock(gateway) do
                sendWorker!(gateway, outbound.identity, outbound.payload)
            end
        catch error
            failPendingCalls!(gateway, outbound.identity, "send to worker failed")
            rethrow(error)
        end
    end
    return nothing
end

function drainOutbound!(gateway::Gateway)
    while isready(gateway.outboundSends)
        take!(gateway.outboundSends)
    end
    return nothing
end

function drainOutbound!(gateway::Gateway, identity::Vector{UInt8})
    kept = OutboundSend[]
    while isready(gateway.outboundSends)
        outbound = take!(gateway.outboundSends)
        outbound.identity == identity || push!(kept, outbound)
    end
    for outbound in kept
        put!(gateway.outboundSends, outbound)
    end
    return nothing
end

function recordIdentity!(gateway::Gateway, workerId::String, identity::Vector{UInt8})
    lock(gateway.identityLock)
    try
        previous = get(gateway.identities, workerId, nothing)
        if !isnothing(previous) && previous != identity
            throw(ZmqError("workerId $(repr(workerId)) is already connected"))
        end
        gateway.identities[workerId] = identity
        return nothing
    finally
        unlock(gateway.identityLock)
    end
end

function workerIdentity(gateway::Gateway, workerId::String)
    identity = lock(gateway.identityLock) do
        get(gateway.identities, workerId, nothing)
    end
    isnothing(identity) && throw(ZmqError("unknown workerId $(repr(workerId))"))
    return identity
end

# ROUTER_NOTIFY delivers [identity, empty] when a peer drops (crash, partition,
# heartbeat timeout). A peer that never sent workerHello has no workerId — ignore it.
function disconnectWorker!(
    gateway::Gateway,
    routerState::Router.RouterState,
    identity::Vector{UInt8},
)
    workerId = workerForIdentity(gateway, identity)
    isnothing(workerId) && return nothing
    forgetWorker!(gateway, routerState, workerId)
    return nothing
end

function workerForIdentity(gateway::Gateway, identity::Vector{UInt8})
    lock(gateway.identityLock) do
        for (workerId, known) in gateway.identities
            known == identity && return workerId
        end
        return nothing
    end
end

function forgetWorker!(gateway::Gateway, routerState::Router.RouterState, workerId::String)
    identity = forgetIdentity!(gateway, workerId)
    Router.unregisterWorker!(routerState, workerId)
    if !isnothing(identity)
        drainOutbound!(gateway, identity)
        failPendingCalls!(gateway, identity, "worker $(repr(workerId)) disconnected")
    end
    return nothing
end

function forgetIdentity!(gateway::Gateway, workerId::String)
    lock(gateway.identityLock)
    try
        return pop!(gateway.identities, workerId, nothing)
    finally
        unlock(gateway.identityLock)
    end
end

function registerCall!(gateway::Gateway, callId::Int, identity::Vector{UInt8})
    channel = Channel{Any}(1)
    lock(gateway.callLock)
    try
        haskey(gateway.pendingCalls, callId) &&
            throw(ZmqError("duplicate pending callId $(callId)"))
        gateway.pendingCalls[callId] = PendingCall(identity, channel)
        return channel
    finally
        unlock(gateway.callLock)
    end
end

function failPendingCalls!(gateway::Gateway, message::String)
    pendingCalls = lock(gateway.callLock) do
        calls = collect(values(gateway.pendingCalls))
        empty!(gateway.pendingCalls)
        calls
    end
    for pending in pendingCalls
        put!(pending.reply, ZmqError(message))
    end
    return nothing
end

function failPendingCalls!(gateway::Gateway, identity::Vector{UInt8}, message::String)
    pendingCalls = lock(gateway.callLock) do
        matched = PendingCall[]
        for (callId, pending) in collect(gateway.pendingCalls)
            pending.identity == identity || continue
            push!(matched, pending)
            delete!(gateway.pendingCalls, callId)
        end
        matched
    end
    for pending in pendingCalls
        put!(pending.reply, ZmqError(message))
    end
    return nothing
end

function completeCall!(
    gateway::Gateway,
    identity::Vector{UInt8},
    callId::Int,
    payload::Vector{UInt8},
)
    pending = lock(gateway.callLock) do
        pending = get(gateway.pendingCalls, callId, nothing)
        isnothing(pending) && return nothing
        pending.identity == identity ||
            throw(ZmqError("worker reply came from unexpected identity"))
        pop!(gateway.pendingCalls, callId)
    end
    isnothing(pending) && return nothing
    put!(pending.reply, payload)
    return nothing
end

function cancelCall!(gateway::Gateway, callId::Int)
    lock(gateway.callLock)
    try
        pop!(gateway.pendingCalls, callId, nothing)
        return nothing
    finally
        unlock(gateway.callLock)
    end
end

# Blocking entry point for the group runner: the process is the gateway.
# The runner owns the process lifecycle; there is no stop message.
function serve(endpoint::String)
    run!(gateway(endpoint), Router.RouterState())
    return nothing
end

function withSocketLock(f, gateway::Gateway)
    lock(gateway.socketLock)
    try
        return f()
    finally
        unlock(gateway.socketLock)
    end
end

end # module Zmq
