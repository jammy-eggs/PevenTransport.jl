# A faithful worker echoes the request's callId, whatever the router assigned;
# echoCallIds=false simulates a byzantine worker for mismatch coverage.
mutable struct FakeGateway
    workerIds::Vector{String}
    messages::Vector{Any}
    replies::Dict{String,Any}
    echoCallIds::Bool
    lock::ReentrantLock
end

FakeGateway(replies::AbstractVector; echoCallIds::Bool=true) = FakeGateway(
    String[],
    Any[],
    Dict(replyRunKey(reply) => reply for reply in replies),
    echoCallIds,
    ReentrantLock(),
)

FakeGateway(replies::AbstractDict; echoCallIds::Bool=true) = FakeGateway(
    String[],
    Any[],
    Dict(String(runKey) => reply for (runKey, reply) in pairs(replies)),
    echoCallIds,
    ReentrantLock(),
)

function PevenTransport.Router.callWorker(
    gateway::FakeGateway,
    workerId::String,
    payload::Vector{UInt8},
)
    message = PevenTransport.IPC.decode(payload)
    reply = lock(gateway.lock) do
        push!(gateway.workerIds, workerId)
        push!(gateway.messages, message)
        runKey = message["ctx"]["bundle"]["runKey"]
        haskey(gateway.replies, runKey) ||
            error("missing fake reply for runKey $(repr(runKey))")
        pop!(gateway.replies, runKey)
    end
    gateway.echoCallIds && (reply["callId"] = message["callId"])
    return PevenTransport.IPC.encode(reply)
end

function replyRunKey(reply)
    outputs = reply["outputs"]
    for bucket in values(outputs)
        isempty(bucket) && continue
        return bucket[1]["runKey"]
    end
    error("fake executorResult reply must include at least one output token")
end

function tauToolCtx(runKey::String)
    Peven = PevenTransport.Peven
    return Peven.ExecutionContext(
        Peven.Bundle(:tool, runKey, nothing),
        7,
        1,
        Dict(
            :toolInput => Peven.Token[
                Peven.Token(:state, runKey, Dict("kind" => "state", "tid" => runKey)),
            ],
            :db => Peven.Token[
                Peven.Token(:db, runKey, Dict("kind" => "db", "tid" => runKey)),
            ],
        ),
    )
end

@testset "Router sends ExecutionContext by runKey" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")

    gateway = FakeGateway(Any[
        PevenTransport.IPC.executorResult(
            1,
            Dict(
                "db" => Any[
                    Dict(
                        "color" => "db",
                        "runKey" => "tau1-002",
                        "payload" => Dict("kind" => "db", "tid" => "tau1-002"),
                    ),
                ],
                "agentInput" => Any[
                    Dict(
                        "color" => "state",
                        "runKey" => "tau1-002",
                        "payload" => Dict("kind" => "state", "tid" => "tau1-002"),
                    ),
                ],
            ),
        ),
    ])

    executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)
    outputs = Peven.execute(executor, tauToolCtx("tau1-002"))

    @test gateway.workerIds == ["workerA"]
    @test only(gateway.messages)["kind"] == "executorCall"
    @test only(gateway.messages)["callId"] == 1
    @test only(gateway.messages)["executorName"] == "tool"
    @test only(gateway.messages)["ctx"]["bundle"]["runKey"] == "tau1-002"
    @test Set(keys(outputs)) == Set([:db, :agentInput])
end

@testset "Router surfaces missing assignment and executor errors" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    gateway = FakeGateway(Dict{String,Any}())
    executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)

    @test_throws PevenTransport.Router.RouterError Peven.execute(executor, tauToolCtx("missing"))

    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "missing", "workerA")
    gateway.replies["missing"] = PevenTransport.IPC.executorError(1, "tool exploded")

    @test_throws PevenTransport.Router.PythonExecutionError Peven.execute(
        executor,
        tauToolCtx("missing"),
    )
end

@testset "Router releases run assignments" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")
    PevenTransport.Router.unroute!(router, "tau1-002")

    gateway = FakeGateway(Any[])
    executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)

    @test_throws PevenTransport.Router.RouterError Peven.execute(
        executor,
        tauToolCtx("tau1-002"),
    )
end

@testset "Router rejects mismatched worker replies" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")
    gateway = FakeGateway(
        Dict("tau1-002" => PevenTransport.IPC.executorResult(99, Dict()));
        echoCallIds=false,
    )
    executor = PevenTransport.Router.PythonExecutor(:tool, router, gateway)

    @test_throws PevenTransport.Router.RouterError Peven.execute(
        executor,
        tauToolCtx("tau1-002"),
    )
end

@testset "Fake gateway replies by runKey instead of insertion order" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "first", "workerA")
    gateway = FakeGateway(Dict(
        "other" => PevenTransport.IPC.executorResult(
            1,
            Dict(
                "done" => Any[
                    Dict("color" => "score", "runKey" => "other", "payload" => nothing),
                ],
            ),
        ),
        "first" => PevenTransport.IPC.executorResult(
            1,
            Dict(
                "done" => Any[
                    Dict("color" => "score", "runKey" => "first", "payload" => nothing),
                ],
            ),
        ),
    ))
    executor = PevenTransport.Router.PythonExecutor(:finishExecutor, router, gateway)

    outputs = Peven.execute(executor, tauToolCtx("first"))

    @test Peven.runKey(only(outputs[:done])) == "first"
end

@testset "Router requires registered workers" begin
    router = PevenTransport.Router.RouterState()

    @test_throws PevenTransport.Router.RouterError PevenTransport.Router.route!(
        router,
        "tau1-002",
        "workerA",
    )

    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")

    @test PevenTransport.Router.workerForRun(router, "tau1-002") == "workerA"
end

@testset "Router unregisters worker assignments" begin
    Peven = PevenTransport.Peven
    router = PevenTransport.Router.RouterState()
    PevenTransport.Router.registerWorker!(router, "workerA")
    PevenTransport.Router.route!(router, "tau1-002", "workerA")
    PevenTransport.Router.unregisterWorker!(router, "workerA")

    executor = PevenTransport.Router.PythonExecutor(
        :tool,
        router,
        FakeGateway(Dict{String,Any}()),
    )

    @test_throws PevenTransport.Router.RouterError Peven.execute(
        executor,
        tauToolCtx("tau1-002"),
    )
end

@testset "Router unregisters unknown workers as no-op" begin
    router = PevenTransport.Router.RouterState()

    PevenTransport.Router.unregisterWorker!(router, "missing")

    @test isempty(router.workers)
    @test isempty(router.runWorkers)
end
