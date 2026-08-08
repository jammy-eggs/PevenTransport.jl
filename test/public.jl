@testset "PevenTransport loads Peven v0.6 API" begin
    @test isdefined(PevenTransport, :Peven)
    @test isdefined(PevenTransport.Peven, :Net)
    @test isdefined(PevenTransport.Peven, :Marking)
    @test isdefined(PevenTransport.Peven, :Token)
    @test isdefined(PevenTransport.Peven, :fire)
    @test isdefined(PevenTransport.Peven, :validate)
    @test isdefined(PevenTransport.Peven, :registerExec!)
    @test isdefined(PevenTransport.Peven, :ExecutionContext)
    @test isdefined(PevenTransport.Peven, :RunResult)
end

@testset "PevenTransport public surface is the Julia bridge" begin
    @test isdefined(PevenTransport, :IPC)
    @test isdefined(PevenTransport, :Router)
    @test isdefined(PevenTransport, :Zmq)
    @test !isdefined(PevenTransport.Zmq, :dealer)

    @test !isdefined(PevenTransport, :Adapter)
    @test !isdefined(PevenTransport, :AuthoredIR)
    @test !isdefined(PevenTransport, :Lowering)
    @test !isdefined(PevenTransport, :Protocol)
    @test !isdefined(PevenTransport, :Session)
    @test !isdefined(PevenTransport, :main)
end
