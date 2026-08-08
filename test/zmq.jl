import Sockets

@testset "ZMQ dispatches worker hello over ROUTER DEALER" begin
    endpoint = "inproc://peventransport-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")

        @test haskey(gateway.identities, "workerA")
        @test "workerA" in router.workers
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ validates executor timeout" begin
    endpoint = "inproc://peventransport-executor-timeout-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)
    try
        @test gateway.executorTimeout == 900.0
    finally
        close(gateway.socket)
    end

    for timeout in (0, NaN, Inf)
        @test_throws ArgumentError PevenTransport.Zmq.gateway(
            "inproc://peventransport-invalid-timeout-$(time_ns())";
            executorTimeout=timeout,
        )
    end
end

@testset "ZMQ contains unroutable sends" begin
    endpoint = "inproc://peventransport-unroutable-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)
    router = PevenTransport.Router.RouterState()
    control = ZMQ.Socket(ZMQ.DEALER)
    control.routing_id = "control"
    control.rcvtimeo = 20
    identity = Vector{UInt8}("control")
    Sockets.connect(control, endpoint)

    try
        @test_throws ZMQ.StateError PevenTransport.Zmq.sendWorker!(
            gateway,
            UInt8[0x01],
            UInt8[0x02],
        )
        Sockets.send(control, UInt8[0x01])
        ZMQ.recv_multipart(gateway.socket, Vector{UInt8})
        put!(
            gateway.outboundSends,
            PevenTransport.Zmq.OutboundSend(UInt8[0x01], UInt8[0x0a]),
        )
        put!(
            gateway.outboundSends,
            PevenTransport.Zmq.OutboundSend(identity, UInt8[0x0b]),
        )

        @test PevenTransport.Zmq.sendOutbound!(gateway) === nothing
        @test Sockets.recv(control, Vector{UInt8}) == UInt8[0x0b]

        Sockets.send(
            control,
            PevenTransport.IPC.encode(PevenTransport.IPC.workerHello("workerA")),
        )
        close(control)
        @test timedwait(
            () -> try
                PevenTransport.Zmq.sendWorker!(gateway, identity, UInt8[0x01])
                false
            catch error
                error isa ZMQ.StateError
            end,
            1.0;
            pollint=0.001,
        ) === :ok
        @test PevenTransport.Zmq.dispatch!(gateway, router) === nothing
    finally
        close(control)
        close(gateway.socket)
    end
end

@testset "ZMQ rejects duplicate worker identities" begin
    endpoint = "inproc://peventransport-duplicate-worker-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)

    try
        connectWorker(gateway, router, workerA, "workerA")

        reply = dispatchReply(gateway, router, workerB, PevenTransport.IPC.workerHello("workerA"))
        @test reply["kind"] == "gatewayError"
    finally
        close(workerA)
        close(workerB)
        close(gateway.socket)
    end
end

@testset "ZMQ accepts repeated worker hello from same identity" begin
    endpoint = "inproc://peventransport-repeat-worker-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        for _ in 1:2
            connectWorker(gateway, router, worker, "workerA")
        end

        @test haskey(gateway.identities, "workerA")
        @test "workerA" in router.workers
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ handles worker goodbye" begin
    endpoint = "inproc://peventransport-worker-goodbye-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")
        PevenTransport.Router.route!(router, "runA", "workerA")

        reply = dispatchReply(gateway, router, worker, PevenTransport.IPC.workerGoodbye("workerA"))

        @test reply == PevenTransport.IPC.workerGone("workerA")
        @test !haskey(gateway.identities, "workerA")
        @test_throws PevenTransport.Router.RouterError PevenTransport.Router.workerForRun(router, "runA")
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ rejects worker goodbye from wrong identity" begin
    endpoint = "inproc://peventransport-worker-goodbye-wrong-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)

    try
        connectWorker(gateway, router, workerA, "workerA")
        PevenTransport.Router.route!(router, "runA", "workerA")

        reply = dispatchReply(gateway, router, workerB, PevenTransport.IPC.workerGoodbye("workerA"))

        @test reply["kind"] == "gatewayError"
        @test haskey(gateway.identities, "workerA")
        @test PevenTransport.Router.workerForRun(router, "runA") == "workerA"
    finally
        close(workerA)
        close(workerB)
        close(gateway.socket)
    end
end

@testset "ZMQ keeps running after bad worker control message" begin
    endpoint = "inproc://peventransport-bad-worker-message-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(workerA, "workerA")
        @test recvMessage(workerA) == PevenTransport.IPC.workerReady("workerA")

        Sockets.send(workerB, PevenTransport.IPC.encode(PevenTransport.IPC.workerGoodbye("workerA")))
        reply = recvMessage(workerB)
        @test reply["kind"] == "gatewayError"
        @test !istaskdone(runTask)

        sendWorkerHello(workerB, "workerB")
        @test recvMessage(workerB) ==
              PevenTransport.IPC.workerReady("workerB")
    finally
        close(workerA)
        close(workerB)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ survives malformed datagrams" begin
    endpoint = "inproc://peventransport-malformed-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        # wrong frame count: a DEALER sending two body frames reaches the
        # ROUTER as three frames, like a REQ socket's empty delimiter would
        ZMQ.send_multipart(worker, [Vector{UInt8}("x"), Vector{UInt8}("y")])
        @test recvMessage(worker)["kind"] == "gatewayError"
        @test !istaskdone(runTask)

        # bytes that are not msgpack (0xc1 is reserved and never valid)
        Sockets.send(worker, UInt8[0xc1])
        @test recvMessage(worker)["kind"] == "gatewayError"
        @test !istaskdone(runTask)

        # well-formed msgpack whose top level is not a map
        Sockets.send(worker, PevenTransport.IPC.encode(42))
        reply = recvMessage(worker)
        @test reply["kind"] == "gatewayError"
        @test reply["error"] == "message must be a map"
        @test !istaskdone(runTask)

        # integer field that overflows Int
        Sockets.send(
            worker,
            PevenTransport.IPC.encode(
                Dict(
                    "kind" => "executorResult",
                    "callId" => typemax(UInt64),
                    "outputs" => Dict(),
                ),
            ),
        )
        reply = recvMessage(worker)
        @test reply["kind"] == "gatewayError"
        @test reply["error"] == "callId must be a positive integer"
        @test !istaskdone(runTask)

        # the loop is still serving well-formed peers
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ rejects unsupported worker messages" begin
    endpoint = "inproc://peventransport-unsupported-message-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        reply = dispatchReply(gateway, router, worker, Dict("kind" => "mystery"))

        @test reply["kind"] == "gatewayError"
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ accepts assign from target worker identity" begin
    endpoint = "inproc://peventransport-assign-owner-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")

        reply = dispatchReply(gateway, router, worker, PevenTransport.IPC.assign("runA", "workerA"))

        @test reply["kind"] == "assigned"
        @test PevenTransport.Router.workerForRun(router, "runA") == "workerA"
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ rejects assign from wrong worker identity" begin
    endpoint = "inproc://peventransport-assign-wrong-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)

    try
        for (worker, workerId) in ((workerA, "workerA"), (workerB, "workerB"))
            connectWorker(gateway, router, worker, workerId)
        end

        reply = dispatchReply(gateway, router, workerB, PevenTransport.IPC.assign("runA", "workerA"))

        @test reply["kind"] == "gatewayError"
        @test_throws PevenTransport.Router.RouterError PevenTransport.Router.workerForRun(router, "runA")
    finally
        close(workerA)
        close(workerB)
        close(gateway.socket)
    end
end

@testset "ZMQ accepts release from owning worker identity" begin
    endpoint = "inproc://peventransport-release-owner-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")
        PevenTransport.Router.route!(router, "runA", "workerA")

        reply = dispatchReply(gateway, router, worker, PevenTransport.IPC.release("runA"))

        @test reply["kind"] == "released"
        @test_throws PevenTransport.Router.RouterError PevenTransport.Router.workerForRun(router, "runA")
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ rejects release from wrong worker identity" begin
    endpoint = "inproc://peventransport-release-wrong-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)

    try
        for (worker, workerId) in ((workerA, "workerA"), (workerB, "workerB"))
            connectWorker(gateway, router, worker, workerId)
        end
        PevenTransport.Router.route!(router, "runA", "workerA")

        reply = dispatchReply(gateway, router, workerB, PevenTransport.IPC.release("runA"))

        @test reply["kind"] == "gatewayError"
        @test PevenTransport.Router.workerForRun(router, "runA") == "workerA"
    finally
        close(workerA)
        close(workerB)
        close(gateway.socket)
    end
end

@testset "ZMQ gateway carries executor calls" begin
    endpoint = "inproc://peventransport-executor-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) ==
              PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
        executeTask = Threads.@spawn PevenTransport.Peven.execute(
            executor,
            tauToolCtx("tau1-002"),
        )

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"
        @test call["executorName"] == "tool"
        @test call["ctx"]["bundle"]["runKey"] == "tau1-002"

        sendExecutorResult(
            worker,
            call,
            "agentInput",
            "tau1-002";
            payload=Dict("kind" => "state", "tid" => "tau1-002"),
        )

        outputs = fetch(executeTask)
        @test Set(keys(outputs)) == Set([:agentInput])
        @test PevenTransport.Peven.runKey(only(outputs[:agentInput])) == "tau1-002"
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ routes executor replies by callId" begin
    endpoint = "inproc://peventransport-callid-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    workerA = dealer(endpoint)
    workerB = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(workerA, "workerA")
        @test recvMessage(workerA) == PevenTransport.IPC.workerReady("workerA")

        sendWorkerHello(workerB, "workerB")
        @test recvMessage(workerB) == PevenTransport.IPC.workerReady("workerB")

        PevenTransport.Router.route!(router, "runA", "workerA")
        PevenTransport.Router.route!(router, "runB", "workerB")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
        taskA = Threads.@spawn PevenTransport.Peven.execute(
            executor,
            tauToolCtx("runA"),
        )
        taskB = Threads.@spawn PevenTransport.Peven.execute(executor, tauToolCtx("runB"))

        callA = recvMessage(workerA)
        callB = recvMessage(workerB)
        @test callA["ctx"]["bundle"]["runKey"] == "runA"
        @test callB["ctx"]["bundle"]["runKey"] == "runB"

        sendExecutorResult(workerB, callB, "done", "runB")
        sendExecutorResult(workerA, callA, "done", "runA")
        @test PevenTransport.Peven.runKey(only(fetch(taskA)[:done])) == "runA"
        @test PevenTransport.Peven.runKey(only(fetch(taskB)[:done])) == "runB"
    finally
        close(workerA)
        close(workerB)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ runs concurrent executor calls on one socket" begin
    endpoint = "inproc://peventransport-serialized-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "runA", "workerA")
        PevenTransport.Router.route!(router, "runB", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)

        taskA = Threads.@spawn PevenTransport.Peven.execute(executor, tauToolCtx("runA"))
        taskB = Threads.@spawn PevenTransport.Peven.execute(executor, tauToolCtx("runB"))

        firstCall = recvMessage(worker)
        firstRunKey = firstCall["ctx"]["bundle"]["runKey"]
        sendExecutorResult(worker, firstCall, "done", firstRunKey)
        secondCall = recvMessage(worker)
        secondRunKey = secondCall["ctx"]["bundle"]["runKey"]
        sendExecutorResult(worker, secondCall, "done", secondRunKey)
        @test Set([firstRunKey, secondRunKey]) == Set(["runA", "runB"])
        @test PevenTransport.Peven.runKey(only(fetch(taskA)[:done])) == "runA"
        @test PevenTransport.Peven.runKey(only(fetch(taskB)[:done])) == "runB"
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ pending calls complete from IPC payloads" begin
    endpoint = "inproc://peventransport-pending-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        identity = UInt8[0x01]
        channel = PevenTransport.Zmq.registerCall!(gateway, 7, identity)
        payload = encodeExecutorResult(7)
        PevenTransport.Zmq.completeCall!(gateway, identity, 7, payload)

        @test take!(channel) == payload
        @test isempty(gateway.pendingCalls)
        PevenTransport.Zmq.completeCall!(
            gateway,
            identity,
            7,
            payload,
        )

        channel = PevenTransport.Zmq.registerCall!(gateway, 8, identity)
        wrongPayload = encodeExecutorResult(8)
        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Zmq.completeCall!(
            gateway,
            UInt8[0x02],
            8,
            wrongPayload,
        )
        @test haskey(gateway.pendingCalls, 8)
        PevenTransport.Zmq.completeCall!(gateway, identity, 8, wrongPayload)
        @test take!(channel) == wrongPayload

        PevenTransport.Zmq.registerCall!(gateway, 9, identity)
        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Zmq.registerCall!(gateway, 9, identity)
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ fails pending calls on gateway stop" begin
    endpoint = "inproc://peventransport-fail-pending-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        channel = PevenTransport.Zmq.registerCall!(gateway, 7, UInt8[0x01])
        PevenTransport.Zmq.failPendingCalls!(gateway, "gateway stopped")

        reply = take!(channel)
        @test reply isa PevenTransport.Zmq.ZmqError
        @test isempty(gateway.pendingCalls)
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ forgets workers and fails their pending calls" begin
    endpoint = "inproc://peventransport-forget-worker-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        identityA = UInt8[0x01]
        identityB = UInt8[0x02]
        PevenTransport.Zmq.recordIdentity!(gateway, "workerA", identityA)
        PevenTransport.Zmq.recordIdentity!(gateway, "workerB", identityB)
        PevenTransport.Router.registerWorker!(router, "workerA")
        PevenTransport.Router.registerWorker!(router, "workerB")
        PevenTransport.Router.route!(router, "runA", "workerA")
        PevenTransport.Router.route!(router, "runB", "workerB")

        channelA = PevenTransport.Zmq.registerCall!(gateway, 1, identityA)
        channelB = PevenTransport.Zmq.registerCall!(gateway, 2, identityB)
        PevenTransport.Zmq.forgetWorker!(gateway, router, "workerA")

        @test !haskey(gateway.identities, "workerA")
        @test haskey(gateway.identities, "workerB")
        @test_throws PevenTransport.Router.RouterError PevenTransport.Router.workerForRun(router, "runA")
        @test PevenTransport.Router.workerForRun(router, "runB") == "workerB"
        @test take!(channelA) isa PevenTransport.Zmq.ZmqError
        @test haskey(gateway.pendingCalls, 2)

        PevenTransport.Zmq.completeCall!(
            gateway,
            identityB,
            2,
            PevenTransport.IPC.encode(
                executorResult(2),
            ),
        )
        @test take!(channelB) isa Vector{UInt8}
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ gateway stop drains outbound sends" begin
    endpoint = "inproc://peventransport-drain-outbound-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        put!(gateway.outboundSends, PevenTransport.Zmq.OutboundSend(UInt8[0x01], UInt8[0x02]))
        PevenTransport.Zmq.drainOutbound!(gateway)

        @test !isready(gateway.outboundSends)
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ send failure fails pending calls for that worker" begin
    endpoint = "inproc://peventransport-send-failure-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    identity = UInt8[0x01]
    channel = PevenTransport.Zmq.registerCall!(gateway, 1, identity)
    put!(gateway.outboundSends, PevenTransport.Zmq.OutboundSend(identity, UInt8[0x02]))
    close(gateway.socket)

    @test_throws Exception PevenTransport.Zmq.sendOutbound!(gateway)
    @test take!(channel) isa PevenTransport.Zmq.ZmqError
    @test isempty(gateway.pendingCalls)
end

@testset "ZMQ rejects stopped gateways" begin
    endpoint = "inproc://peventransport-stopped-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)
    router = PevenTransport.Router.RouterState()

    try
        PevenTransport.Zmq.stop!(gateway)
        @test gateway.lifecycle == :closed
        error = try
            PevenTransport.Zmq.run!(gateway, router)
            nothing
        catch error
            error
        end
        @test error isa PevenTransport.Zmq.ZmqError
        @test error.message == "gateway is stopped"
        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            PevenTransport.IPC.encode(PevenTransport.IPC.executorCall(1, :tool, tauToolCtx("runA"))),
        )
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ rejects executor calls before gateway runs" begin
    endpoint = "inproc://peventransport-call-before-run-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        PevenTransport.Zmq.recordIdentity!(gateway, "workerA", UInt8[0x01])
        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            PevenTransport.IPC.encode(PevenTransport.IPC.executorCall(1, :tool, tauToolCtx("runA"))),
        )
    finally
        close(gateway.socket)
    end
end

@testset "ZMQ replies wake pending calls immediately" begin
    endpoint = "inproc://peventransport-call-wakeup-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        identity = UInt8[0x01]
        PevenTransport.Zmq.recordIdentity!(gateway, "workerA", identity)
        PevenTransport.Zmq.startGateway!(gateway)
        warmPayload = PevenTransport.IPC.encode(
            PevenTransport.IPC.executorCall(1, :tool, tauToolCtx("runA")),
        )
        warmTask = Threads.@spawn PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            warmPayload,
        )
        take!(gateway.outboundSends)
        PevenTransport.Zmq.completeCall!(gateway, identity, 1, encodeExecutorResult(1))
        fetch(warmTask)

        payload = PevenTransport.IPC.encode(
            PevenTransport.IPC.executorCall(2, :tool, tauToolCtx("runA")),
        )
        task = Threads.@spawn PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            payload,
        )
        take!(gateway.outboundSends)
        sleep(0.01)

        PevenTransport.Zmq.completeCall!(gateway, identity, 2, encodeExecutorResult(2))
        sleep(0.05)

        @test istaskdone(task)
        @test fetch(task) == encodeExecutorResult(2)
    finally
        PevenTransport.Zmq.markClosed!(gateway)
        close(gateway.socket)
    end
end

@testset "ZMQ skips canceled queued executor calls" begin
    endpoint = "inproc://peventransport-canceled-queued-call-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")
        PevenTransport.Zmq.startGateway!(gateway)
        payload = PevenTransport.IPC.encode(
            PevenTransport.IPC.executorCall(7, :tool, tauToolCtx("runA")),
        )
        task = Threads.@spawn PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            payload,
        )
        @test timedwait(() -> isready(gateway.outboundSends), 1.0) === :ok

        PevenTransport.Zmq.timeoutCall!(gateway, 7)
        @test_throws TaskFailedException fetch(task)
        PevenTransport.Zmq.sendOutbound!(gateway)

        worker.rcvtimeo = 20
        @test_throws ZMQ.TimeoutError recvMessage(worker)
    finally
        PevenTransport.Zmq.markClosed!(gateway)
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ cancels calls if gateway stops before send" begin
    endpoint = "inproc://peventransport-stop-before-send-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        PevenTransport.Zmq.recordIdentity!(gateway, "workerA", UInt8[0x01])
        PevenTransport.Zmq.startGateway!(gateway)
        PevenTransport.Zmq.stop!(gateway)

        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            PevenTransport.IPC.encode(PevenTransport.IPC.executorCall(1, :tool, tauToolCtx("runA"))),
        )
        @test isempty(gateway.pendingCalls)
    finally
        PevenTransport.Zmq.markClosed!(gateway)
        close(gateway.socket)
    end
end

@testset "ZMQ cancels calls if worker disconnects before send" begin
    endpoint = "inproc://peventransport-worker-gone-before-send-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)

    try
        identity = UInt8[0x01]
        PevenTransport.Zmq.recordIdentity!(gateway, "workerA", identity)
        PevenTransport.Router.registerWorker!(router, "workerA")
        PevenTransport.Zmq.startGateway!(gateway)
        PevenTransport.Zmq.forgetWorker!(gateway, router, "workerA")

        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Router.callWorker(
            gateway,
            "workerA",
            PevenTransport.IPC.encode(PevenTransport.IPC.executorCall(1, :tool, tauToolCtx("runA"))),
        )
        @test isempty(gateway.pendingCalls)
        @test !isready(gateway.outboundSends)
    finally
        PevenTransport.Zmq.markClosed!(gateway)
        close(gateway.socket)
    end
end

@testset "ZMQ rejects already running gateways" begin
    endpoint = "inproc://peventransport-running-$(time_ns())"
    gateway = PevenTransport.Zmq.gateway(endpoint)
    router = PevenTransport.Router.RouterState()
    PevenTransport.Zmq.startGateway!(gateway)

    try
        @test_throws PevenTransport.Zmq.ZmqError PevenTransport.Zmq.run!(gateway, router)
    finally
        PevenTransport.Zmq.markClosed!(gateway)
        close(gateway.socket)
    end
end

@testset "ZMQ gateway stop fails waiting executor calls" begin
    endpoint = "inproc://peventransport-stop-executor-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) ==
              PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
        executeTask = Threads.@spawn PevenTransport.Peven.execute(
            executor,
            tauToolCtx("tau1-002"),
        )

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"

        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
        error = try
            fetch(executeTask)
            nothing
        catch error
            error
        end
        @test error isa TaskFailedException
        @test error.task.result isa PevenTransport.Zmq.ZmqError
    finally
        close(worker)
        if !istaskdone(runTask)
            PevenTransport.Zmq.stop!(gateway)
            fetch(runTask)
        end
    end
end

@testset "ZMQ dispatch completes executor replies" begin
    endpoint = "inproc://peventransport-dispatch-reply-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        sendWorkerHello(worker, "workerA")
        helloTask = Threads.@spawn PevenTransport.Zmq.dispatch!(gateway, router)
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")
        fetch(helloTask)

        identity = gateway.identities["workerA"]
        channel = PevenTransport.Zmq.registerCall!(gateway, 12, identity)
        payload = encodeExecutorResult(12)

        Sockets.send(worker, payload)
        PevenTransport.Zmq.dispatch!(gateway, router)

        @test take!(channel) == payload
        @test isempty(gateway.pendingCalls)
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ ignores replies for canceled calls" begin
    endpoint = "inproc://peventransport-stale-reply-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)

    try
        connectWorker(gateway, router, worker, "workerA")
        identity = gateway.identities["workerA"]
        PevenTransport.Zmq.registerCall!(gateway, 12, identity)
        PevenTransport.Zmq.cancelCall!(gateway, 12)
        Sockets.send(worker, encodeExecutorResult(12))
        PevenTransport.Zmq.dispatch!(gateway, router)

        connectWorker(gateway, router, worker, "workerA")
    finally
        close(worker)
        close(gateway.socket)
    end
end

@testset "ZMQ run dispatches worker traffic in the background" begin
    endpoint = "inproc://peventransport-run-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    @test gateway.socket.rcvtimeo == 1
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) ==
              PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
        executeTask = Threads.@spawn PevenTransport.Peven.execute(
            executor,
            tauToolCtx("tau1-002"),
        )

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"
        sendExecutorResult(worker, call, "done", "tau1-002")

        @test PevenTransport.Peven.runKey(only(fetch(executeTask)[:done])) == "tau1-002"
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ worker disconnect fails pending calls and frees the worker" begin
    endpoint = "tcp://127.0.0.1:$(50000 + time_ns() % 10000)"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) ==
              PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
        executeTask = Threads.@spawn PevenTransport.Peven.execute(
            executor,
            tauToolCtx("tau1-002"),
        )

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"

        close(worker)  # crash, not goodbye: TCP close must surface as ROUTER_NOTIFY

        error = try
            fetch(executeTask)
            nothing
        catch error
            error
        end
        @test error isa TaskFailedException
        @test error.task.result isa PevenTransport.Zmq.ZmqError
        @test error.task.result.message == "worker \"workerA\" disconnected"

        @test_throws PevenTransport.Router.RouterError PevenTransport.Router.workerForRun(
            router,
            "tau1-002",
        )

        replacement = dealer(endpoint)
        try
            sendWorkerHello(replacement, "workerA")
            @test recvMessage(replacement) ==
                  PevenTransport.IPC.workerReady("workerA")
        finally
            close(replacement)
        end
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

function loweredControlNet(name::String)
    return Dict(
        "name" => name,
        "places" => [
            Dict("id" => "prompt", "capacity" => nothing),
            Dict("id" => "done", "capacity" => nothing),
        ],
        "transitions" => [Dict("id" => "solve", "executor" => "solve")],
        "arcsFrom" => [Dict(
            "transition" => "solve", "from" => "prompt",
            "weight" => 1, "optional" => false,
        )],
        "arcsTo" => [Dict("transition" => "solve", "to" => "done", "weight" => 1)],
    )
end

function controlMarking(runKey::String)
    return Dict(
        "tokensByPlace" => Dict(
            "prompt" => [tokenMessage("question", runKey, Dict("q" => "2+2"))],
        ),
    )
end

@testset "ZMQ control client loads nets" begin
    Peven = PevenTransport.Peven
    endpoint = "inproc://peventransport-load-net-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    control = dealer(endpoint)
    sentinel = Peven.FunctionExecutor(_ -> nothing)
    Peven.registerExec!(:solve, sentinel)

    try
        reply = dispatchReply(
            gateway, router, control,
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        )
        @test reply == PevenTransport.IPC.netLoaded("ctrl")
        @test haskey(gateway.nets, "ctrl")
        @test Peven.getExec(:solve) === sentinel

        dangling = loweredControlNet("broken")
        dangling["arcsFrom"][1]["from"] = "ghost"
        rejected = dispatchReply(
            gateway, router, control,
            PevenTransport.IPC.loadNet(dangling),
        )
        @test rejected["kind"] == "gatewayError"
        @test startswith(rejected["error"], "invalid net \"broken\"")
        @test !haskey(gateway.nets, "broken")
    finally
        Peven.unregisterExec!(:solve)
        close(control)
        close(gateway.socket)
    end
end

@testset "ZMQ fire rejects before launch with correlated errors" begin
    endpoint = "inproc://peventransport-fire-reject-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    control = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.fire("groupA", "ghost", controlMarking("ctrl#g0")),
        ))
        @test recvMessage(control) ==
              PevenTransport.IPC.fireFinished("groupA", "unknown net \"ghost\"")

        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.netLoaded("ctrl")

        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.fire("groupA", "ctrl", controlMarking("ctrl#g0")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.fireFinished(
            "groupA",
            "no worker assigned for runKey \"ctrl#g0\"",
        )
    finally
        close(control)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ fire streams run results to the control client" begin
    endpoint = "inproc://peventransport-fire-stream-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    control = dealer(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")
        Sockets.send(worker, PevenTransport.IPC.encode(
            PevenTransport.IPC.assign("ctrl#g0", "workerA"),
        ))
        @test recvMessage(worker) == PevenTransport.IPC.assigned("ctrl#g0", "workerA")

        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.netLoaded("ctrl")
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.fire("groupA", "ctrl", controlMarking("ctrl#g0")),
        ))

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"
        @test call["executorName"] == "solve"
        @test call["ctx"]["bundle"]["runKey"] == "ctrl#g0"
        sendExecutorResult(worker, call, "done", "ctrl#g0"; payload="4")

        finished = recvMessage(control)
        @test finished["kind"] == "runFinished"
        @test finished["fireId"] == "groupA"
        result = finished["result"]
        @test result["runKey"] == "ctrl#g0"
        @test result["status"] == "completed"
        @test result["finalMarking"]["tokensByPlace"]["done"] ==
              [Dict("color" => "state", "runKey" => "ctrl#g0", "payload" => "4")]
        @test only(result["trace"])["bundle"]["transitionId"] == "solve"

        @test recvMessage(control) == PevenTransport.IPC.fireFinished("groupA", nothing)
        @test isempty(gateway.activeFires)
    finally
        close(control)
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "ZMQ serve runs a gateway from one call" begin
    endpoint = "inproc://peventransport-serve-$(time_ns())"
    # serve blocks by design (the runner owns the process lifecycle), so the
    # task has no stop handle and idles until test-process exit.
    Threads.@spawn PevenTransport.serve(endpoint)
    control = dealer(endpoint)

    try
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("served")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.netLoaded("served")
    finally
        close(control)
    end
end

@testset "ZMQ rejects loadNet while a fire is active" begin
    endpoint = "inproc://peventransport-load-during-fire-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    control = dealer(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")
        Sockets.send(worker, PevenTransport.IPC.encode(
            PevenTransport.IPC.assign("ctrl#g0", "workerA"),
        ))
        @test recvMessage(worker) == PevenTransport.IPC.assigned("ctrl#g0", "workerA")
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.netLoaded("ctrl")
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.fire("groupA", "ctrl", controlMarking("ctrl#g0")),
        ))
        call = recvMessage(worker)
        @test call["kind"] == "executorCall"

        # fire is in flight: the worker is holding the call un-replied
        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        ))
        rejected = recvMessage(control)
        @test rejected["kind"] == "gatewayError"
        @test rejected["error"] == "cannot load a net while fires are active"

        sendExecutorResult(worker, call, "done", "ctrl#g0"; payload="4")
        @test recvMessage(control)["kind"] == "runFinished"
        @test recvMessage(control) == PevenTransport.IPC.fireFinished("groupA", nothing)

        Sockets.send(control, PevenTransport.IPC.encode(
            PevenTransport.IPC.loadNet(loweredControlNet("ctrl")),
        ))
        @test recvMessage(control) == PevenTransport.IPC.netLoaded("ctrl")
    finally
        close(control)
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end
