@testset "IPC executorCall mirrors ExecutionContext" begin
    Peven = PevenTransport.Peven
    ctx = Peven.ExecutionContext(
        Peven.Bundle(:tool, "tau1-002", nothing),
        7,
        1,
        Dict(
            :toolInput => Peven.Token[
                Peven.Token(
                    :state,
                    "tau1-002",
                    Dict("kind" => "state", "tid" => "tau1-002"),
                ),
            ],
            :db => Peven.Token[
                Peven.Token(
                    :db,
                    "tau1-002",
                    Dict("kind" => "db", "tid" => "tau1-002"),
                ),
            ],
        ),
    )

    message = PevenTransport.IPC.executorCall(11, :tool, ctx)

    @test message["kind"] == "executorCall"
    @test message["callId"] == 11
    @test message["executorName"] == "tool"
    @test message["ctx"]["bundle"] == Dict(
        "transitionId" => "tool",
        "runKey" => "tau1-002",
        "selectedKey" => nothing,
    )
    @test message["ctx"]["firingId"] == 7
    @test message["ctx"]["attempt"] == 1
    @test message["ctx"]["inputs"]["db"][1]["payload"] ==
          Dict("kind" => "db", "tid" => "tau1-002")
    @test !haskey(message, "env")
    @test !haskey(message, "store")
    @test !haskey(message, "tokens")

    smallId = PevenTransport.IPC.executorCall(UInt8(11), :tool, ctx)
    @test smallId["callId"] == 11
    @test smallId["callId"] isa Int

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorCall(0, :tool, ctx)
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorCall(true, :tool, ctx)
end

@testset "IPC executorResult decodes grouped outputs" begin
    Peven = PevenTransport.Peven
    outputs = PevenTransport.IPC.decodeExecutorResult(Dict(
        "kind" => "executorResult",
        "callId" => 11,
        "outputs" => Dict(
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
    ))

    @test Set(keys(outputs)) == Set([:db, :agentInput])
    @test Peven.runKey(only(outputs[:db])) == "tau1-002"
    @test getfield(only(outputs[:agentInput]), :payload) ==
          Dict("kind" => "state", "tid" => "tau1-002")
end

@testset "IPC executor reply messages" begin
    outputs = Dict("done" => Any[])
    @test PevenTransport.IPC.executorResult(11, outputs) == Dict(
        "kind" => "executorResult",
        "callId" => 11,
        "outputs" => outputs,
    )
    @test PevenTransport.IPC.executorError(11, "tool exploded") == Dict(
        "kind" => "executorError",
        "callId" => 11,
        "error" => "tool exploded",
    )
    @test PevenTransport.IPC.decodeExecutorError(
        PevenTransport.IPC.executorError(11, "tool exploded"),
    ) == "tool exploded"

    @test PevenTransport.IPC.executorResult(UInt8(11), outputs)["callId"] isa Int
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorResult(0, outputs)
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorResult(11, [])
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorError(0, "tool exploded")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.executorError(11, "")
end

@testset "IPC worker handshake messages" begin
    hello = PevenTransport.IPC.workerHello("workerA")
    @test hello == Dict(
        "kind" => "workerHello",
        "workerId" => "workerA",
        "protocol" => 1,
    )
    @test PevenTransport.IPC.decodeWorkerHello(hello) == "workerA"

    ready = PevenTransport.IPC.workerReady("workerA")
    @test ready == Dict(
        "kind" => "workerReady",
        "workerId" => "workerA",
    )

    goodbye = PevenTransport.IPC.workerGoodbye("workerA")
    @test goodbye == Dict(
        "kind" => "workerGoodbye",
        "workerId" => "workerA",
    )
    @test PevenTransport.IPC.decodeWorkerGoodbye(goodbye) == "workerA"

    gone = PevenTransport.IPC.workerGone("workerA")
    @test gone == Dict(
        "kind" => "workerGone",
        "workerId" => "workerA",
    )

    error = PevenTransport.IPC.gatewayError("bad message")
    @test error == Dict(
        "kind" => "gatewayError",
        "error" => "bad message",
    )

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.workerHello("")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.workerReady("")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.workerGoodbye("")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.workerGone("")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.gatewayError("")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeWorkerHello(Dict(
        "kind" => "workerHello",
        "workerId" => "",
    ))
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeWorkerHello(Dict(
        "kind" => "workerHello",
        "workerId" => "workerA",
    ))
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeWorkerHello(Dict(
        "kind" => "workerHello",
        "workerId" => "workerA",
        "protocol" => 2,
    ))
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeWorkerGoodbye(Dict(
        "kind" => "workerGoodbye",
        "workerId" => "",
    ))
end

@testset "IPC control handshake loads nets" begin
    lowered = Dict(
        "name" => "e2e",
        "places" => [Dict("id" => "prompt", "capacity" => nothing)],
        "transitions" => Dict[],
        "arcsFrom" => Dict[],
        "arcsTo" => Dict[],
    )
    loaded = PevenTransport.IPC.decodeLoadNet(PevenTransport.IPC.loadNet(lowered))
    @test loaded.name == "e2e"
    @test collect(keys(loaded.net.places)) == [:prompt]

    @test PevenTransport.IPC.netLoaded("e2e") == Dict(
        "kind" => "netLoaded",
        "name" => "e2e",
    )

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.loadNet("net")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.netLoaded("")
    unnamed = Dict(k => v for (k, v) in lowered if k != "name")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeLoadNet(
        PevenTransport.IPC.loadNet(unnamed),
    )
end

@testset "IPC encodes run results for the control client" begin
    Peven = PevenTransport.Peven
    bundle = Peven.Bundle(:solve, "task-003#g0", nothing)
    done = Peven.Token(:answer, "task-003#g0", "4")
    result = Peven.RunResult(
        "task-003#g0",
        :completed,
        nothing,
        nothing,
        [
            Peven.TransitionResult(bundle, 1, :completed, [done], nothing, 1),
            Peven.TransitionResult(bundle, 2, :failed, Peven.Token[], "boom", 2),
        ],
        Peven.Marking(Dict(:done => [done])),
    )

    message = PevenTransport.IPC.runFinished("task-003", result)
    @test message["kind"] == "runFinished"
    @test message["fireId"] == "task-003"
    encoded = message["result"]
    @test encoded["runKey"] == "task-003#g0"
    @test encoded["status"] == "completed"
    @test isnothing(encoded["error"])
    @test isnothing(encoded["reason"])
    @test encoded["finalMarking"]["tokensByPlace"] == Dict(
        "done" => [Dict("color" => "answer", "runKey" => "task-003#g0", "payload" => "4")],
    )
    completed, failed = encoded["trace"]
    @test completed["bundle"]["transitionId"] == "solve"
    @test completed["status"] == "completed"
    @test only(completed["outputs"])["payload"] == "4"
    @test completed["attempts"] == 1
    @test failed["status"] == "failed"
    @test failed["error"] == "boom"
    @test isempty(failed["outputs"])

    @test PevenTransport.IPC.fireFinished("task-003", nothing) == Dict(
        "kind" => "fireFinished",
        "fireId" => "task-003",
        "error" => nothing,
    )
    @test PevenTransport.IPC.fireFinished("task-003", "no such net")["error"] ==
          "no such net"
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.fireFinished("", nothing)
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.fireFinished("task-003", "")
end

@testset "IPC pins selectedKey to msgpack scalars" begin
    Peven = PevenTransport.Peven
    keyed = PevenTransport.IPC.bundleMessage(Peven.Bundle(:judge, "task-003#g0", :gold))
    @test keyed["selectedKey"] == "gold"
    @test PevenTransport.IPC.bundleMessage(
        Peven.Bundle(:judge, "task-003#g0", 7),
    )["selectedKey"] == 7

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.bundleMessage(
        Peven.Bundle(:judge, "task-003#g0", (1, 2)),
    )
end

@testset "IPC fire messages cross with engine defaults" begin
    marking = Dict(
        "tokensByPlace" => Dict(
            "prompt" => [Dict("color" => "question", "runKey" => "task-003#g0")],
        ),
    )
    bare = PevenTransport.IPC.decodeFire(PevenTransport.IPC.fire("task-003", "tau", marking))
    @test bare.fireId == "task-003"
    @test bare.net == "tau"
    @test PevenTransport.Peven.runKey(only(bare.marking.tokensByPlace[:prompt])) ==
          "task-003#g0"
    @test bare.fuse == 1000
    @test bare.maxConcurrency == 10

    tuned = PevenTransport.IPC.decodeFire(
        PevenTransport.IPC.fire("task-003", "tau", marking; fuse=80, maxConcurrency=4),
    )
    @test tuned.fuse == 80
    @test tuned.maxConcurrency == 4

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.fire("", "tau", marking)
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.fire("task-003", "", marking)
    badFuse = PevenTransport.IPC.fire("task-003", "tau", marking)
    badFuse["fuse"] = 0
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeFire(badFuse)
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeFire(Dict(
        "kind" => "fire",
        "fireId" => "task-003",
        "net" => "tau",
    ))
end

@testset "IPC decodes lowered nets" begin
    lowered = Dict(
        "name" => "e2e",
        "places" => [
            Dict("id" => "prompt", "capacity" => nothing),
            Dict("id" => "done", "capacity" => 2),
        ],
        "transitions" => [Dict("id" => "solve", "executor" => "solve")],
        "arcsFrom" => [Dict(
            "transition" => "solve", "from" => "prompt",
            "weight" => 1, "optional" => false,
        )],
        "arcsTo" => [Dict("transition" => "solve", "to" => "done", "weight" => 1)],
    )
    net = PevenTransport.IPC.decodeNet(lowered)
    @test isnothing(net.places[:prompt].capacity)
    @test net.places[:done].capacity == 2
    @test net.transitions[:solve].executor == :solve
    @test isnothing(net.transitions[:solve].guard)
    @test net.transitions[:solve].retries == 0
    @test only(net.arcsfrom) == PevenTransport.Peven.ArcFrom(:solve, :prompt, 1)
    @test only(net.arcsto) == PevenTransport.Peven.ArcTo(:solve, :done, 1)

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeNet(Dict())
    badWeight = deepcopy(lowered)
    badWeight["arcsTo"][1]["weight"] = 0
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeNet(badWeight)
    boolCapacity = deepcopy(lowered)
    boolCapacity["places"][2]["capacity"] = true
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeNet(boolCapacity)
end

@testset "IPC decodes lowered markings" begin
    marking = PevenTransport.IPC.decodeMarking(Dict(
        "tokensByPlace" => Dict(
            "prompt" => [Dict(
                "color" => "question",
                "runKey" => "task-003#g0",
                "payload" => Dict("q" => "2+2"),
            )],
        ),
    ))
    token = only(marking.tokensByPlace[:prompt])
    @test PevenTransport.Peven.color(token) == :question
    @test PevenTransport.Peven.runKey(token) == "task-003#g0"
    @test token.payload == Dict("q" => "2+2")

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeMarking(Dict())
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decodeMarking("marking")
end

@testset "IPC assign messages" begin
    message = PevenTransport.IPC.assign("tau1-002", "workerA")
    @test message == Dict(
        "kind" => "assign",
        "runKey" => "tau1-002",
        "workerId" => "workerA",
    )

    decoded = PevenTransport.IPC.decodeAssign(message)
    @test decoded.runKey == "tau1-002"
    @test decoded.workerId == "workerA"

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.assign("", "workerA")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.assign("tau1-002", "")

    @test PevenTransport.IPC.assigned("tau1-002", "workerA") == Dict(
        "kind" => "assigned",
        "runKey" => "tau1-002",
        "workerId" => "workerA",
    )
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.assigned("", "workerA")
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.assigned("tau1-002", "")
end

@testset "IPC release messages" begin
    message = PevenTransport.IPC.release("tau1-002")
    @test message == Dict(
        "kind" => "release",
        "runKey" => "tau1-002",
    )

    @test PevenTransport.IPC.decodeRelease(message) == "tau1-002"
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.release("")

    @test PevenTransport.IPC.released("tau1-002") == Dict(
        "kind" => "released",
        "runKey" => "tau1-002",
    )
    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.released("")
end

@testset "IPC MessagePack roundtrip" begin
    message = PevenTransport.IPC.assign("tau1-002", "workerA")
    payload = PevenTransport.IPC.encode(message)

    @test payload isa Vector{UInt8}
    @test PevenTransport.IPC.decode(payload) == message
end

@testset "IPC rejects oversized payloads" begin
    oversized = fill(UInt8(0), PevenTransport.IPC.maxPayloadBytes + 1)

    @test_throws PevenTransport.IPC.IpcError PevenTransport.IPC.decode(oversized)
end
