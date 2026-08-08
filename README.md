# PevenTransport.jl

`PevenTransport.jl` is the Julia-side transport bridge between Python rollout
workers and `Peven.jl`.

## Current Shape

`PevenTransport` has three internal pieces:

- `IPC`: MessagePack message construction and decoding.
- `Router`: `runKey` to worker routing plus `Peven.execute` integration.
- `Zmq`: a ZMQ `ROUTER` gateway for Python `DEALER` workers.

The package imports and exposes its compatible `Peven.jl` dependency as
`PevenTransport.Peven` for callers that need the exact engine API.

## Architecture Pattern

The design follows the same broad pattern used by systems like Ray, SGLang, and
vLLM, but scoped to Peven execution:

- Workers own mutable execution state. Julia does not inspect or mutate Python
  rollout objects directly.
- The gateway owns routing. A `runKey` is assigned to one worker, and executor
  calls for that run go back to that worker.
- Calls are correlated by `callId`, so executor replies can complete out of
  order without being matched to the wrong Peven firing.
- ZMQ is only the transport. The protocol semantics are the PevenTransport IPC
  messages, not raw socket frames.

## IPC Contract

Workers connect to the Julia gateway with ZMQ `DEALER` sockets. The gateway uses
a ZMQ `ROUTER` socket and routes executor calls by worker identity.

Current message flow:

- `loadNet` -> `netLoaded`
- `fire` -> `runFinished` messages, then `fireFinished`
- `workerHello` -> `workerReady`
- `assign` -> `assigned`
- `executorCall` -> `executorResult` or `executorError`
- `release` -> `released`
- `workerGoodbye` -> `workerGone`

Malformed non-empty messages and non-fire protocol rejections receive a
best-effort `gatewayError`; once a fire request decodes, rejection or completion
closes with a correlated `fireFinished`. Protocol version `2` is required during
the worker handshake. ZMQ heartbeats plus libzmq's draft `ROUTER_NOTIFY`
disconnect notifications retire worker identities; workers are not restarted.

Executor calls carry:

- `callId`: positive integer used to correlate worker replies.
- `executorName`: string name of the `Peven.jl` executor.
- `ctx`: serialized `Peven.ExecutionContext`.

The serialized context includes the current bundle, firing id, attempt, and
input token buckets. Tokens preserve `color`, `runKey`, and `payload`.

## Version

This repo targets `Peven.jl` `0.6.x` and is versioned as `0.2.0`.

## Tests

Run the test suite with:

```sh
julia --project=. -e 'include("test/runtests.jl")'
```
