module IPC

using MsgPack
using Peven

const maxPayloadBytes = 8 * 1024 * 1024
const protocolVersion = 2

struct IpcError <: Exception
    message::String
end

Base.showerror(io::IO, error::IpcError) = print(io, error.message)

function encode(message)
    payload = MsgPack.pack(message)
    length(payload) <= maxPayloadBytes ||
        throw(IpcError("IPC payload exceeds $(maxPayloadBytes) bytes"))
    return payload
end

function decode(payload::AbstractVector{UInt8})
    length(payload) <= maxPayloadBytes ||
        throw(IpcError("IPC payload exceeds $(maxPayloadBytes) bytes"))
    return MsgPack.unpack(payload)
end

function executorCall(callId::Integer, executorName::Symbol, ctx::Peven.ExecutionContext)
    id = requireCallId(callId)
    return Dict(
        "kind" => "executorCall",
        "callId" => id,
        "executorName" => String(executorName),
        "ctx" => contextMessage(ctx),
    )
end

function workerHello(workerId::String)
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "workerHello",
        "workerId" => workerId,
        "protocol" => protocolVersion,
    )
end

function workerReady(workerId::String)
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "workerReady",
        "workerId" => workerId,
    )
end

function workerGoodbye(workerId::String)
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "workerGoodbye",
        "workerId" => workerId,
    )
end

function workerGone(workerId::String)
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "workerGone",
        "workerId" => workerId,
    )
end

function gatewayError(error::String)
    isempty(error) && throw(IpcError("error must be non-empty"))
    return Dict(
        "kind" => "gatewayError",
        "error" => error,
    )
end

function decodeWorkerHello(message)
    map = requireKind(message, "workerHello")
    workerId = requireString(map, "workerId")
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    get(map, "protocol", nothing) == protocolVersion ||
        throw(IpcError("workerHello protocol must be $(protocolVersion)"))
    return workerId
end

function decodeWorkerGoodbye(message)
    map = requireKind(message, "workerGoodbye")
    workerId = requireString(map, "workerId")
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return workerId
end

function assign(runKey::String, workerId::String)
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "assign",
        "runKey" => runKey,
        "workerId" => workerId,
    )
end

function assigned(runKey::String, workerId::String)
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return Dict(
        "kind" => "assigned",
        "runKey" => runKey,
        "workerId" => workerId,
    )
end

function decodeAssign(message)
    map = requireKind(message, "assign")
    runKey = requireString(map, "runKey")
    workerId = requireString(map, "workerId")
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    isempty(workerId) && throw(IpcError("workerId must be non-empty"))
    return (; runKey, workerId)
end

function release(runKey::String)
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    return Dict(
        "kind" => "release",
        "runKey" => runKey,
    )
end

function released(runKey::String)
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    return Dict(
        "kind" => "released",
        "runKey" => runKey,
    )
end

function decodeRelease(message)
    map = requireKind(message, "release")
    runKey = requireString(map, "runKey")
    isempty(runKey) && throw(IpcError("runKey must be non-empty"))
    return runKey
end

function contextMessage(ctx::Peven.ExecutionContext)
    return Dict(
        "bundle" => bundleMessage(ctx.bundle),
        "firingId" => ctx.firingId,
        "attempt" => ctx.attempt,
        "inputs" => tokenBuckets(ctx.inputs),
    )
end

function decodeExecutorResult(message)
    map = requireKind(message, "executorResult")
    return decodeTokenBuckets(requireMapField(map, "outputs"))
end

function executorResult(callId::Integer, outputs)
    id = requireCallId(callId)
    requireMap(outputs, "outputs")
    return Dict(
        "kind" => "executorResult",
        "callId" => id,
        "outputs" => outputs,
    )
end

function decodeExecutorError(message)
    map = requireKind(message, "executorError")
    error = requireString(map, "error")
    isempty(error) && throw(IpcError("error must be non-empty"))
    return error
end

function executorError(callId::Integer, error::String)
    id = requireCallId(callId)
    isempty(error) && throw(IpcError("error must be non-empty"))
    return Dict(
        "kind" => "executorError",
        "callId" => id,
        "error" => error,
    )
end

function callId(message)
    map = requireMap(message, "message")
    value = get(map, "callId", nothing)
    return requireCallId(value)
end

function requireCallId(value)
    value isa Integer && !(value isa Bool) || throw(IpcError("callId must be an integer"))
    0 < value <= typemax(Int) || throw(IpcError("callId must be a positive integer"))
    return Int(value)
end

function bundleMessage(bundle::Peven.Bundle)
    return Dict(
        "transitionId" => String(bundle.transitionId),
        "runKey" => bundle.runKey,
        "selectedKey" => selectedKeyMessage(bundle.selectedKey),
    )
end

function selectedKeyMessage(key)
    key isa Union{Nothing,AbstractString,Symbol,Integer,Bool} ||
        throw(IpcError("selectedKey must be a string, integer, bool, or nothing"))
    return key isa Symbol ? String(key) : key
end

function tokenBuckets(buckets)
    return Dict(
        String(placeId) => [tokenMessage(token) for token in bucket]
        for (placeId, bucket) in buckets
    )
end

function tokenMessage(token::Peven.Token)
    return Dict(
        "color" => String(Peven.color(token)),
        "runKey" => Peven.runKey(token),
        "payload" => getfield(token, :payload),
    )
end

function decodeTokenBuckets(buckets)
    decoded = Dict{Symbol,Vector{Peven.Token}}()
    sizehint!(decoded, length(buckets))
    for (placeId, bucket) in pairs(buckets)
        placeId isa AbstractString || throw(IpcError("place ids must be strings"))
        bucket isa AbstractVector || throw(IpcError("token buckets must be lists"))
        decoded[Symbol(placeId)] = [decodeToken(token) for token in bucket]
    end
    return decoded
end

function loadNet(net)
    return Dict(
        "kind" => "loadNet",
        "net" => requireMap(net, "net"),
    )
end

function decodeLoadNet(message)
    map = requireKind(message, "loadNet")
    netMap = requireMapField(map, "net")
    name = requireString(netMap, "name")
    isempty(name) && throw(IpcError("name must be non-empty"))
    return (; name, net = decodeNet(netMap))
end

function fire(fireId::String, net::String, marking; fuse=nothing, maxConcurrency=nothing)
    isempty(fireId) && throw(IpcError("fireId must be non-empty"))
    isempty(net) && throw(IpcError("net must be non-empty"))
    message = Dict{String,Any}(
        "kind" => "fire",
        "fireId" => fireId,
        "net" => net,
        "marking" => requireMap(marking, "marking"),
    )
    isnothing(fuse) || (message["fuse"] = fuse)
    isnothing(maxConcurrency) || (message["maxConcurrency"] = maxConcurrency)
    return message
end

function decodeFire(message)
    map = requireKind(message, "fire")
    fireId = requireString(map, "fireId")
    net = requireString(map, "net")
    isempty(fireId) && throw(IpcError("fireId must be non-empty"))
    isempty(net) && throw(IpcError("net must be non-empty"))
    marking = decodeMarking(requireMapField(map, "marking"))
    fuse = haskey(map, "fuse") ? requirePositiveInt(map, "fuse") : 1000
    maxConcurrency =
        haskey(map, "maxConcurrency") ? requirePositiveInt(map, "maxConcurrency") : 10
    return (; fireId, net, marking, fuse, maxConcurrency)
end

function netLoaded(name::String)
    isempty(name) && throw(IpcError("name must be non-empty"))
    return Dict(
        "kind" => "netLoaded",
        "name" => name,
    )
end

function runFinished(fireId::String, result::Peven.RunResult)
    isempty(fireId) && throw(IpcError("fireId must be non-empty"))
    return Dict(
        "kind" => "runFinished",
        "fireId" => fireId,
        "result" => runResultMessage(result),
    )
end

function fireFinished(fireId::String, error::Union{Nothing,String})
    isempty(fireId) && throw(IpcError("fireId must be non-empty"))
    isnothing(error) || !isempty(error) || throw(IpcError("error must be non-empty"))
    return Dict(
        "kind" => "fireFinished",
        "fireId" => fireId,
        "error" => error,
    )
end

function runResultMessage(result::Peven.RunResult)
    return Dict(
        "runKey" => result.runKey,
        "status" => String(result.status),
        "error" => result.error,
        "reason" => isnothing(result.reason) ? nothing : String(result.reason),
        "trace" => [transitionResultMessage(entry) for entry in result.trace],
        "finalMarking" => Dict(
            "tokensByPlace" => tokenBuckets(result.finalMarking.tokensByPlace),
        ),
    )
end

function transitionResultMessage(entry::Peven.TransitionResult)
    return Dict(
        "bundle" => bundleMessage(entry.bundle),
        "firingId" => entry.firingId,
        "status" => String(entry.status),
        "outputs" => [tokenMessage(token) for token in entry.outputs],
        "error" => entry.error,
        "attempts" => entry.attempts,
    )
end

function decodeNet(message)
    map = requireMap(message, "net")
    places = Dict{Symbol,Peven.Place}()
    for row in requireListField(map, "places")
        place = requireMap(row, "place")
        id = Symbol(requireString(place, "id"))
        haskey(places, id) && throw(IpcError("duplicate place id :$id"))
        places[id] = Peven.Place(id, decodeCapacity(place))
    end
    transitions = Dict{Symbol,Peven.Transition}()
    for row in requireListField(map, "transitions")
        transition = requireMap(row, "transition")
        id = Symbol(requireString(transition, "id"))
        haskey(transitions, id) && throw(IpcError("duplicate transition id :$id"))
        transitions[id] = Peven.Transition(
            id,
            Symbol(requireString(transition, "executor"));
            retries=decodeRetries(transition),
        )
    end
    arcsFrom = Peven.ArcFrom[]
    for row in requireListField(map, "arcsFrom")
        arc = requireMap(row, "arcFrom")
        push!(arcsFrom, Peven.ArcFrom(
            Symbol(requireString(arc, "transition")),
            Symbol(requireString(arc, "from")),
            requirePositiveInt(arc, "weight");
            optional = requireBool(arc, "optional"),
        ))
    end
    arcsTo = Peven.ArcTo[]
    for row in requireListField(map, "arcsTo")
        arc = requireMap(row, "arcTo")
        push!(arcsTo, Peven.ArcTo(
            Symbol(requireString(arc, "transition")),
            Symbol(requireString(arc, "to")),
            requirePositiveInt(arc, "weight"),
        ))
    end
    return Peven.Net(places, transitions, arcsFrom, arcsTo)
end

function decodeCapacity(place)
    isnothing(get(place, "capacity", nothing)) && return nothing
    return requirePositiveInt(place, "capacity")
end

function decodeRetries(transition)
    retries = get(transition, "retries", 0)
    retries isa Integer && !(retries isa Bool) && 0 <= retries <= typemax(Int) ||
        throw(IpcError("retries must be a non-negative integer"))
    return Int(retries)
end

function decodeMarking(message)
    map = requireMap(message, "marking")
    return Peven.Marking(decodeTokenBuckets(requireMapField(map, "tokensByPlace")))
end

function decodeToken(value)
    token = requireMap(value, "token")
    return Peven.Token(
        Symbol(requireString(token, "color")),
        requireString(token, "runKey"),
        get(token, "payload", nothing),
    )
end

function requireKind(message, expected::String)
    map = requireMap(message, "message")
    kind = requireString(map, "kind")
    kind == expected || throw(IpcError("expected kind $(repr(expected))"))
    return map
end

function requireMapField(message, key::String)
    haskey(message, key) || throw(IpcError("missing required field $(repr(key))"))
    return requireMap(message[key], key)
end

function requireString(message, key::String)
    haskey(message, key) || throw(IpcError("missing required field $(repr(key))"))
    value = message[key]
    value isa AbstractString || throw(IpcError("$(key) must be a string"))
    return String(value)
end

function requireMap(value, context::String)
    value isa AbstractDict || throw(IpcError("$(context) must be a map"))
    return value
end

function requireListField(message, key::String)
    haskey(message, key) || throw(IpcError("missing required field $(repr(key))"))
    value = message[key]
    value isa AbstractVector || throw(IpcError("$(key) must be a list"))
    return value
end

function requirePositiveInt(message, key::String)
    haskey(message, key) || throw(IpcError("missing required field $(repr(key))"))
    value = message[key]
    value isa Integer && !(value isa Bool) && 0 < value <= typemax(Int) ||
        throw(IpcError("$(key) must be a positive integer"))
    return Int(value)
end

function requireBool(message, key::String)
    haskey(message, key) || throw(IpcError("missing required field $(repr(key))"))
    value = message[key]
    value isa Bool || throw(IpcError("$(key) must be a boolean"))
    return value
end

# --- inbound boundary ---------------------------------------------------------
# parseWorkerMessage is total over peer bytes: every datagram becomes exactly one
# typed message or Malformed — it never throws on input. Handlers downstream only
# ever see well-formed messages, so bytes off the wire cannot kill the dispatch
# loop, and any exception raised past this point is a deliberate protocol-state
# rejection or an internal bug.

struct WorkerHello
    workerId::String
end

struct WorkerGoodbye
    workerId::String
end

struct Assign
    runKey::String
    workerId::String
end

struct Release
    runKey::String
end

# executorResult | executorError: the payload is forwarded raw to the pending
# call's channel; output decoding stays in the executor task, where a bad reply
# fails one call instead of the gateway.
struct ExecutorReply
    callId::Int
    payload::Vector{UInt8}
end

struct LoadNet
    name::String
    net::Peven.Net
end

struct Fire
    fireId::String
    net::String
    marking::Peven.Marking
    fuse::Int
    maxConcurrency::Int
end

struct Malformed
    reason::String
end

function parseWorkerMessage(payload::AbstractVector{UInt8})
    try
        message = requireMap(decode(payload), "message")
        kind = requireString(message, "kind")
        if kind == "executorResult" || kind == "executorError"
            return ExecutorReply(callId(message), Vector{UInt8}(payload))
        elseif kind == "workerHello"
            return WorkerHello(decodeWorkerHello(message))
        elseif kind == "workerGoodbye"
            return WorkerGoodbye(decodeWorkerGoodbye(message))
        elseif kind == "assign"
            assignment = decodeAssign(message)
            return Assign(assignment.runKey, assignment.workerId)
        elseif kind == "release"
            return Release(decodeRelease(message))
        elseif kind == "loadNet"
            loaded = decodeLoadNet(message)
            return LoadNet(loaded.name, loaded.net)
        elseif kind == "fire"
            request = decodeFire(message)
            return Fire(
                request.fireId,
                request.net,
                request.marking,
                request.fuse,
                request.maxConcurrency,
            )
        end
        throw(IpcError("unsupported worker message kind $(repr(kind))"))
    catch error
        error isa InterruptException && rethrow()
        return Malformed(sprint(showerror, error))
    end
end

end # module IPC
