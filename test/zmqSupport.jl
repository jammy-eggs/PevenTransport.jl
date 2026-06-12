import Sockets
import ZMQ

function dealer(endpoint::String)
    socket = ZMQ.Socket(ZMQ.DEALER)
    Sockets.connect(socket, endpoint)
    return socket
end

function recvMessage(socket)
    return PevenTransport.IPC.decode(Sockets.recv(socket, Vector{UInt8}))
end

function dispatchReply(gateway, router, socket, message)
    Sockets.send(socket, PevenTransport.IPC.encode(message))
    task = Threads.@spawn PevenTransport.Zmq.dispatch!(gateway, router)
    reply = recvMessage(socket)
    fetch(task)
    return reply
end

function connectWorker(gateway, router, socket, workerId::String)
    reply = dispatchReply(gateway, router, socket, PevenTransport.IPC.workerHello(workerId))
    @test reply == PevenTransport.IPC.workerReady(workerId)
    return nothing
end

function sendWorkerHello(socket, workerId::String)
    Sockets.send(socket, PevenTransport.IPC.encode(PevenTransport.IPC.workerHello(workerId)))
    return nothing
end

function tokenMessage(color::String, runKey::String, payload)
    return Dict(
        "color" => color,
        "runKey" => runKey,
        "payload" => payload,
    )
end

function executorResult(callId, outputName::String, token)
    return PevenTransport.IPC.executorResult(callId, Dict(outputName => Any[token]))
end

function executorResult(callId::Integer)
    return PevenTransport.IPC.executorResult(callId, Dict())
end

function encodeExecutorResult(callId::Integer)
    return PevenTransport.IPC.encode(executorResult(callId))
end

function sendExecutorResult(socket, call, outputName::String, runKey::String; payload=nothing)
    message = executorResult(
        call["callId"],
        outputName,
        tokenMessage("state", runKey, payload),
    )
    Sockets.send(socket, PevenTransport.IPC.encode(message))
    return nothing
end

function sendExecutorError(socket, call, error::String)
    Sockets.send(socket, PevenTransport.IPC.encode(PevenTransport.IPC.executorError(call["callId"], error)))
    return nothing
end
