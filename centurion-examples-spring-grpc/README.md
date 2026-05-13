# centurion-examples-spring-grpc

A minimal Spring Boot + Spring gRPC application demonstrating
[`centurion-spring-grpc`](../centurion-spring-grpc). Exposes a single
`example.FortuneCookie` service that returns a random fortune.

## What's in here

- `FortuneCookie.kt` — `FortuneRequest` and `FortuneResponse` data classes.
  These are plain Kotlin data classes; centurion-avro builds reflection-based
  encoders/decoders for them at runtime.
- `FortuneCookieService.kt` — the service implementation. Builds the
  `ServerServiceDefinition` using `AvroMethodDescriptors.unary(...)` from
  `centurion-spring-grpc`, so the wire format is Avro binary.
- `FortuneCookieApplication.kt` — `@SpringBootApplication` entry point. The
  `centurion-spring-grpc` auto-configuration picks up any
  `ServerServiceDefinition` bean and attaches it to the embedded gRPC server.
- `application.yml` — server configuration (port 9090 by default).

## Running

```
./gradlew :centurion-examples-spring-grpc:bootRun
```

The service binds to `0.0.0.0:9090` and exposes one unary RPC:

| Full method                 | Request           | Response                    |
|-----------------------------|-------------------|-----------------------------|
| `example.FortuneCookie/Pick` | `FortuneRequest`  | `FortuneResponse`           |

Pass `FortuneRequest(seed = 42L)` for a deterministic response (useful for
testing); `seed = 0L` (the default) uses the system random.

## Calling the service from Kotlin

The same `AvroMethodDescriptors.unary<Req, Resp>(...)` factory works on the
client side — pair it with `io.grpc.stub.ClientCalls.blockingUnaryCall` or
the async variants. See `FortuneCookieServiceTest` for an in-process example.
