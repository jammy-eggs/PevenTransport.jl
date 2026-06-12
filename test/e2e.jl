function simpleNet()
    Peven = PevenTransport.Peven
    return Peven.Net(
        Dict(
            :ready => Peven.Place(:ready),
            :done => Peven.Place(:done),
        ),
        Dict(
            :finish => Peven.Transition(:finish, :finishExecutor),
        ),
        Peven.ArcFrom[Peven.ArcFrom(:finish, :ready)],
        Peven.ArcTo[Peven.ArcTo(:finish, :done)],
    )
end

function simpleMarking()
    Peven = PevenTransport.Peven
    return Peven.Marking(
        Dict(
            :ready => Peven.Token[
                Peven.Token(
                    :state,
                    "tau1-002",
                    Dict("kind" => "state", "tid" => "tau1-002"),
                ),
            ],
        ),
    )
end

function withExec(f, name::Symbol, executor)
    Peven = PevenTransport.Peven
    previous = try
        Peven.getExec(name)
    catch error
        error isa KeyError || rethrow()
        nothing
    end

    Peven.registerExec!(name, executor)
    try
        return f()
    finally
        if isnothing(previous)
            Peven.unregisterExec!(name)
        else
            Peven.registerExec!(name, previous)
        end
    end
end

@testset "Router executor runs through Peven.fire" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")
    gateway = FakeGateway(Any[
        PevenTransport.IPC.executorResult(
            1,
            Dict(
                "done" => Any[
                    Dict(
                        "color" => "score",
                        "runKey" => "tau1-002",
                        "payload" => Dict("reward" => 1.0),
                    ),
                ],
            ),
        ),
    ])

    executor = PevenTransport.Router.PythonExecutor(:finishExecutor, router, gateway)
    results = withExec(:finishExecutor, executor) do
        Peven.fire(simpleNet(), simpleMarking())
    end

    result = only(results)
    @test result.status === :completed
    @test result.runKey == "tau1-002"
    @test haskey(result.finalMarking.tokensByPlace, :done)
    @test getfield(only(result.finalMarking.tokensByPlace[:done]), :payload) ==
          Dict("reward" => 1.0)

    @test gateway.workerIds == ["workerA"]
    @test only(gateway.messages)["kind"] == "executorCall"
    @test only(gateway.messages)["ctx"]["bundle"]["transitionId"] == "finish"
    @test only(gateway.messages)["ctx"]["bundle"]["runKey"] == "tau1-002"
    @test only(gateway.messages)["ctx"]["inputs"]["ready"][1]["payload"] ==
          Dict("kind" => "state", "tid" => "tau1-002")
end

@testset "Peven.fire executes through ZMQ worker" begin
    Peven = PevenTransport.Peven
    endpoint = "inproc://peventransport-fire-zmq-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:finishExecutor, router, gateway)
        fireTask = Threads.@spawn withExec(:finishExecutor, executor) do
            Peven.fire(simpleNet(), simpleMarking())
        end

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"
        @test call["ctx"]["bundle"]["runKey"] == "tau1-002"
        @test call["ctx"]["bundle"]["transitionId"] == "finish"

        sendExecutorResult(
            worker,
            call,
            "done",
            "tau1-002";
            payload=Dict("reward" => 1.0),
        )

        result = only(fetch(fireTask))
        @test result.status === :completed
        @test result.runKey == "tau1-002"
        @test getfield(only(result.finalMarking.tokensByPlace[:done]), :payload) ==
              Dict("reward" => 1.0)
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "Peven.fire fails through ZMQ worker error" begin
    Peven = PevenTransport.Peven
    endpoint = "inproc://peventransport-fire-zmq-error-$(time_ns())"
    router = PevenTransport.Router.RouterState()
    gateway = PevenTransport.Zmq.gateway(endpoint)
    worker = dealer(endpoint)
    runTask = Threads.@spawn PevenTransport.Zmq.run!(gateway, router)

    try
        sendWorkerHello(worker, "workerA")
        @test recvMessage(worker) == PevenTransport.IPC.workerReady("workerA")

        PevenTransport.Router.route!(router, "tau1-002", "workerA")
        executor = PevenTransport.Router.PythonExecutor(:finishExecutor, router, gateway)
        fireTask = Threads.@spawn withExec(:finishExecutor, executor) do
            Peven.fire(simpleNet(), simpleMarking())
        end

        call = recvMessage(worker)
        @test call["kind"] == "executorCall"
        @test call["ctx"]["bundle"]["runKey"] == "tau1-002"

        sendExecutorError(worker, call, "tool exploded")

        result = only(fetch(fireTask))
        @test result.status === :failed
        @test result.reason === :executorFailed
        @test result.error == "tool exploded"
    finally
        close(worker)
        PevenTransport.Zmq.stop!(gateway)
        fetch(runTask)
    end
end

@testset "Router runs multiple runKeys through one Peven.fire" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.registerWorker!(router, "workerB")
    PevenTransport.Router.route!(router, "tau1-002#g0", "workerA")
    PevenTransport.Router.route!(router, "tau1-002#g1", "workerB")
    gateway = FakeGateway(Dict(
        "tau1-002#g1" => PevenTransport.IPC.executorResult(
            2,
            Dict(
                "done" => Any[
                    Dict(
                        "color" => "score",
                        "runKey" => "tau1-002#g1",
                        "payload" => Dict("reward" => 0.0, "worker" => "workerB"),
                    ),
                ],
            ),
        ),
        "tau1-002#g0" => PevenTransport.IPC.executorResult(
            1,
            Dict(
                "done" => Any[
                    Dict(
                        "color" => "score",
                        "runKey" => "tau1-002#g0",
                        "payload" => Dict("reward" => 1.0, "worker" => "workerA"),
                    ),
                ],
            ),
        ),
    ))
    marking = Peven.Marking(
        Dict(
            :ready => Peven.Token[
                Peven.Token(
                    :state,
                    "tau1-002#g0",
                    Dict("kind" => "state", "tid" => "tau1-002"),
                ),
                Peven.Token(
                    :state,
                    "tau1-002#g1",
                    Dict("kind" => "state", "tid" => "tau1-002"),
                ),
            ],
        ),
    )

    executor = PevenTransport.Router.PythonExecutor(:finishExecutor, router, gateway)
    results = withExec(:finishExecutor, executor) do
        Peven.fire(simpleNet(), marking; maxConcurrency=2)
    end

    byRun = Dict(result.runKey => result for result in results)
    @test Set(keys(byRun)) == Set(["tau1-002#g0", "tau1-002#g1"])
    @test byRun["tau1-002#g0"].status === :completed
    @test byRun["tau1-002#g1"].status === :completed

    routed = Dict(
        message["ctx"]["bundle"]["runKey"] => workerId
        for (workerId, message) in zip(gateway.workerIds, gateway.messages)
    )
    @test routed == Dict(
        "tau1-002#g0" => "workerA",
        "tau1-002#g1" => "workerB",
    )
end
