# centurion-ktor

A Ktor [`ContentConverter`](https://api.ktor.io/ktor-shared/ktor-serialization/io.ktor.serialization/-content-converter/index.html)
that uses centurion-avro to serialize and deserialize HTTP bodies as Avro binary.

Works for both the **server-side** (`ktor-server-content-negotiation`) and
**client-side** (`ktor-client-content-negotiation`) Content Negotiation plugins
— Ktor's `ContentConverter` SPI is shared between them, so the same
`AvroContentConverter` covers both directions.

## Installation

```kotlin
implementation("com.sksamuel.centurion:centurion-ktor:<version>")
```

You still need to depend on whichever Content Negotiation plugin you actually use:

```kotlin
implementation("io.ktor:ktor-server-content-negotiation:<ktor-version>")   // server side
implementation("io.ktor:ktor-client-content-negotiation:<ktor-version>")   // client side
```

## Server

```kotlin
import com.sksamuel.centurion.ktor.avro
import io.ktor.server.application.*
import io.ktor.server.plugins.contentnegotiation.*
import io.ktor.server.response.*
import io.ktor.server.routing.*

fun Application.module() {
    install(ContentNegotiation) {
        avro()                              // registers `application/avro`
    }

    routing {
        get("/user/{id}") {
            call.respond(User(id = 1, name = "Ada"))
        }
    }
}
```

## Client

```kotlin
import com.sksamuel.centurion.ktor.avro
import io.ktor.client.*
import io.ktor.client.call.*
import io.ktor.client.plugins.contentnegotiation.*
import io.ktor.client.request.*

val client = HttpClient {
    install(ContentNegotiation) {
        avro()
    }
}

val user: User = client.get("/user/1").body()
```

## Custom content type

Default registration uses `application/avro`. To register on a different media type:

```kotlin
install(ContentNegotiation) {
    avro(contentType = ContentType("application", "vnd.example.user.v1+avro"))
}
```

## Notes

- Built on `BinarySerde` (schemaless binary). Producers and consumers must agree on
  the schema for each type — typically by sharing the data class.
- A `CachingSerdeFactory` is used by default, so each `KClass` only pays the
  reflection cost once across the application lifetime.
