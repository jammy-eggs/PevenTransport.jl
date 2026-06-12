module Router

using Peven

using ..IPC

struct RouterError <: Exception
    message::String
end

Base.showerror(io::IO, error::RouterError) = print(io, error.message)

struct PythonExecutionError <: Exception
    message::String
end

Base.showerror(io::IO, error::PythonExecutionError) = print(io, error.message)

mutable struct RouterState
    workers::Set{String}
    runWorkers::Dict{String,String}
    nextCallId::Int
    lock::ReentrantLock
end

RouterState() = RouterState(Set{String}(), Dict{String,String}(), 1, ReentrantLock())

struct PythonExecutor{TTransport} <: Peven.AbstractExecutor
    name::Symbol
    router::RouterState
    gateway::TTransport
end

function registerWorker!(router::RouterState, workerId::String)
    isempty(workerId) && throw(RouterError("workerId must be non-empty"))
    lock(router.lock)
    try
        push!(router.workers, workerId)
    finally
        unlock(router.lock)
    end
    return nothing
end

function unregisterWorker!(router::RouterState, workerId::String)
    lock(router.lock)
    try
        delete!(router.workers, workerId)
        filter!(pair -> pair.second != workerId, router.runWorkers)
    finally
        unlock(router.lock)
    end
    return nothing
end

function route!(router::RouterState, runKey::String, workerId::String)
    isempty(runKey) && throw(RouterError("runKey must be non-empty"))
    isempty(workerId) && throw(RouterError("workerId must be non-empty"))
    lock(router.lock)
    try
        workerId in router.workers ||
            throw(RouterError("workerId $(repr(workerId)) is not registered"))
        router.runWorkers[runKey] = workerId
    finally
        unlock(router.lock)
    end
    return nothing
end

function unroute!(router::RouterState, runKey::String)
    lock(router.lock)
    try
        delete!(router.runWorkers, runKey)
    finally
        unlock(router.lock)
    end
    return nothing
end

function Peven.execute(executor::PythonExecutor, ctx::Peven.ExecutionContext)
    workerId = workerForRun(executor.router, ctx.bundle.runKey)
    callId = nextCallId!(executor.router)
    message = IPC.executorCall(callId, executor.name, ctx)
    reply = IPC.decode(callWorker(executor.gateway, workerId, IPC.encode(message)))
    return decodeWorkerReply(reply, callId)
end

function workerForRun(router::RouterState, runKey::String)
    workerId = lock(router.lock) do
        get(router.runWorkers, runKey, nothing)
    end
    isnothing(workerId) && throw(RouterError("no worker assigned for runKey $(repr(runKey))"))
    return workerId
end

function isRouted(router::RouterState, runKey::String)
    lock(router.lock) do
        haskey(router.runWorkers, runKey)
    end
end

function nextCallId!(router::RouterState)
    lock(router.lock)
    try
        callId = router.nextCallId
        router.nextCallId += 1
        return callId
    finally
        unlock(router.lock)
    end
end

function callWorker(gateway, workerId::String, payload::Vector{UInt8})
    throw(RouterError("gateway must implement callWorker"))
end

function decodeWorkerReply(reply, callId::Int)
    reply isa AbstractDict || throw(RouterError("worker reply must be a map"))
    requireReplyCallId(reply, callId)
    kind = get(reply, "kind", nothing)
    kind == "executorResult" && return IPC.decodeExecutorResult(reply)
    kind == "executorError" && throw(PythonExecutionError(IPC.decodeExecutorError(reply)))
    throw(RouterError("unsupported worker reply kind $(repr(kind))"))
end

function requireReplyCallId(reply, expected::Int)
    IPC.callId(reply) == expected ||
        throw(RouterError("worker reply callId did not match request"))
    return nothing
end

end # module Router
