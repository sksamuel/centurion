# Centurion <img src="logo.png" height="50">

![master](https://github.com/sksamuel/centurion/workflows/master/badge.svg)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.centurion/centurion-avro.svg?label=latest%20release"/>](https://central.sonatype.com/artifact/com.sksamuel.centurion/centurion-avro)
[<img src="https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Fcentral.sonatype.com%2Frepository%2Fmaven-snapshots%2Fcom%2Fsksamuel%2Fcenturion%2Fcenturion-avro%2Fmaven-metadata.xml&strategy=highestVersion&label=maven-snapshot">](https://central.sonatype.com/repository/maven-snapshots/com/sksamuel/centurion/centurion-avro/maven-metadata.xml)
![License](https://img.shields.io/github/license/sksamuel/centurion.svg?style=plastic)

Centurion is a high-performance Kotlin toolkit for working with Apache Avro in a type-safe,
idiomatic way. It provides zero-copy serialization, automatic code generation,
and seamless integration with modern JVM applications.

## Why Centurion?

- **Type-safe by design:** Leverage Kotlin's type system with compile-time guarantees and automatic null safety
- **Zero-copy performance:** Optimized encoders/decoders with reflection caching and pooled resources
- **Schema evolution made easy:** First-class support for forward/backward compatible schema changes
- **Batteries included:** Support for 40+ types out of the box including temporal types, BigDecimal, collections
- **Production ready:** Originally built for production use at [Grindr](https://grindr.com)

## Features

- **Type-safe schema definitions:** Define schemas using Kotlin's type system with compile-time safety
- **Avro format support:** Binary and data file I/O for Apache Avro
- **High-performance Serde API:** Zero-copy serialization with automatic compression support
- **Schema evolution:** Forward and backward compatible schema changes for Avro
- **Code generation:** Generate data classes and optimized encoders/decoders from Avro schemas
- **Redis integration:** Built-in Lettuce codecs for caching Avro data
- **Streaming operations:** Efficient streaming readers and writers for large datasets
- **Kotlin-first design:** Idiomatic APIs with null safety, data classes, and extension functions

## Getting Started

Add Centurion to your build:

```kotlin
implementation("com.sksamuel.centurion:centurion-avro:<version>")
```

## Quick Start

Here's a complete example to get you started:

```kotlin
import com.sksamuel.centurion.avro.io.serde.BinarySerde
import java.math.BigDecimal

// Define your domain model
data class Product(
    val id: Long,
    val name: String,
    val price: BigDecimal,
    val inStock: Boolean,
    val tags: List<String>
)

// Create a serde (serializer/deserializer)
val serde = BinarySerde<Product>()

// Your data
val product = Product(
    id = 12345L,
    name = "Kotlin in Action",
    price = BigDecimal("39.99"),
    inStock = true,
    tags = listOf("books", "programming", "kotlin")
)

// Serialize to bytes
val bytes = serde.serialize(product)

// Deserialize back to object
val restored = serde.deserialize(bytes)
println(restored) // Product(id=12345, name=Kotlin in Action, ...)
```

## Avro Operations

### Writing Avro Data

```kotlin
import com.sksamuel.centurion.Schema
import com.sksamuel.centurion.Struct
import com.sksamuel.centurion.avro.io.BinaryWriter
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.toAvroSchema
import org.apache.avro.io.EncoderFactory
import java.io.FileOutputStream

// Define your schema
val schema = Schema.Struct(
  Schema.Field("id", Schema.Int64),
  Schema.Field("name", Schema.Strings),
  Schema.Field("timestamp", Schema.TimestampMillis)
)

// Create some data
val records = listOf(
  Struct(schema, 1L, "Alice", System.currentTimeMillis()),
  Struct(schema, 2L, "Bob", System.currentTimeMillis()),
  Struct(schema, 3L, "Charlie", System.currentTimeMillis())
)

// Write to Avro binary format
FileOutputStream("users.avro").use { output ->
  val avroSchema = schema.toAvroSchema()
  val writer = BinaryWriter(
    schema = avroSchema,
    out = output,
    ef = EncoderFactory.get(),
    encoder = ReflectionRecordEncoder(avroSchema, Struct::class),
    reuse = null
  )
  records.forEach { writer.write(it) }
  writer.close()
}
```

### Reading Avro Data

```kotlin
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import org.apache.avro.io.DecoderFactory
import java.io.FileInputStream

// Read from Avro binary format
FileInputStream("users.avro").use { input ->
  val avroSchema = schema.toAvroSchema()
  val reader = BinaryReader(
    schema = avroSchema,
    input = input,
    factory = DecoderFactory.get(),
    decoder = ReflectionRecordDecoder(avroSchema, Struct::class),
    reuse = null
  )
  // BinaryReader reads one record per file
  val struct = reader.read()
  println("User: ${struct["name"]}, ID: ${struct["id"]}")
}
```

## Advanced Types

### Working with Complex Types

```kotlin
// Array/List schema
val numbersSchema = Schema.Array(Schema.Int32)

// Map schema
val metadataSchema = Schema.Map(Schema.Strings) // String keys, String values

// Nested struct
val addressSchema = Schema.Struct(
  Schema.Field("street", Schema.Strings),
  Schema.Field("city", Schema.Strings),
  Schema.Field("zipcode", Schema.Strings)
)

val personSchema = Schema.Struct(
  Schema.Field("name", Schema.Strings),
  Schema.Field("address", addressSchema),
  Schema.Field("phone_numbers", Schema.Array(Schema.Strings))
)
```

### Temporal Types

```kotlin
// Timestamp types
val eventSchema = Schema.Struct(
  Schema.Field("event_name", Schema.Strings),
  Schema.Field("timestamp_millis", Schema.TimestampMillis),
  Schema.Field("timestamp_micros", Schema.TimestampMicros)
)

// Create struct with temporal data
val event = Struct(
  eventSchema,
  "user_login",
  System.currentTimeMillis(),
  System.currentTimeMillis() * 1000
)
```

### Decimal Precision

```kotlin
// High-precision decimal for financial data
val transactionSchema = Schema.Struct(
  Schema.Field("transaction_id", Schema.Strings),
  Schema.Field("amount", Schema.Decimal(
    Schema.Precision(18), // 18 total digits
    Schema.Scale(4)       // 4 decimal places
  ))
)

val transaction = Struct(
  transactionSchema,
  "TXN-123456",
  java.math.BigDecimal("1234.5678")
)
```

## Supported Types

Centurion provides built-in encoders and decoders for a comprehensive set of types:

### Avro Type Support

| Type                           | Encoder/Decoder | Notes                                                                              |
|--------------------------------|-----------------|------------------------------------------------------------------------------------|
| **Primitives**                 |                 |                                                                                    |
| `Byte`, `Short`                | ✓               | Stored as Avro `INT`; decoders widen from narrower numeric types                   |
| `Int`, `Long`                  | ✓               | Direct mapping; `LongDecoder` accepts `Int`/`Short`/`Byte`                         |
| `Float`, `Double`              | ✓               | IEEE 754 floating point; `DoubleDecoder` accepts `Float`                           |
| `Boolean`                      | ✓               |                                                                                    |
| **Strings**                    |                 |                                                                                    |
| `String`                       | ✓               | Schema-driven: encoded as `STRING` (UTF-8/Utf8), `BYTES`, or `FIXED`               |
| `CharSequence`                 | Decoder only    | `CharSequenceDecoder` — coerces incoming UTF-8/`Utf8`/`String` without conversion  |
| `Utf8`                         | Decoder only    | `UTF8Decoder` — keeps the Avro-native UTF-8 representation                         |
| `UUID`                         | Encoder only    | `Utf8UUIDEncoder` (default) or `JavaStringUUIDEncoder`                             |
| **Temporal Types**             |                 |                                                                                    |
| `Instant`                      | ✓               | `TimestampMillis`/`TimestampMicros` logical types or raw `LONG` epoch-millis       |
| `LocalDateTime`                | ✓               | `LocalTimestampMillis`/`LocalTimestampMicros` logical types or raw `LONG`          |
| `LocalTime`                    | ✓               | `TimeMillis` (`INT`) / `TimeMicros` (`LONG`) logical types                         |
| `OffsetDateTime`               | ✓               | Round-trips through `Instant` at UTC                                               |
| **Numeric Types**              |                 |                                                                                    |
| `BigDecimal`                   | ✓               | Separate encoders/decoders for `BYTES`, `FIXED`, and `STRING` representations      |
| **Collections**                |                 |                                                                                    |
| `List<T>`, `Set<T>`            | ✓               | Generic support; primitive/`String` element types use a zero-cost passthrough path |
| `Array<T>`                     | Encoder only    | `ArrayEncoder` — decoder side returns `List<T>`                                    |
| `IntArray`, `LongArray`        | ✓               | Specialized primitive arrays, no boxing                                            |
| `Map<String, T>`               | ✓               | Avro requires `String` keys; preserves insertion order                             |
| **Binary**                     |                 |                                                                                    |
| `ByteArray`                    | ✓               | Encoded as `BYTES` or `FIXED` (zero-padded when shorter than `fixedSize`)          |
| `ByteBuffer`                   | ✓               | Honours `position`/`limit`; never mutates the source buffer                        |
| **Other**                      |                 |                                                                                    |
| Enum classes                   | ✓               | Any Kotlin or Java `enum` — uses Avro `ENUM` symbols                               |
| Nullable types (`T?`)          | ✓               | Encoded as a 2-element union with `null`; full Kotlin null-safety                  |
| Data classes                   | ✓               | Via reflection (`ReflectionRecord{En,De}coder`) or generated code                  |

## High-Performance Serde API

The Serde (Serializer/Deserializer) API provides a convenient way to convert between Kotlin objects and byte arrays with minimal overhead:

```kotlin
import com.sksamuel.centurion.avro.io.serde.BinarySerde

// Create a serde for your data class
data class User(val id: Long, val name: String, val email: String?)

val serde = BinarySerde<User>()

// Serialize to bytes
val user = User(123L, "Alice", "alice@example.com")
val bytes = serde.serialize(user)

// Deserialize from bytes
val decoded = serde.deserialize(bytes)
```

### Compression Support

Apply compression transparently with `CompressingSerde`:

```kotlin
import com.sksamuel.centurion.avro.io.serde.CompressingSerde
import org.apache.avro.file.CodecFactory

val serde = CompressingSerde(
    codec = CodecFactory.snappyCodec().createInstance(),
    serde = BinarySerde<User>()
)

// Automatically compresses on serialize, decompresses on deserialize
val compressed = serde.serialize(user)
```

### Serde Factory Pattern

For applications managing multiple schemas:

```kotlin
import com.sksamuel.centurion.avro.io.serde.SerdeFactory
import com.sksamuel.centurion.avro.io.serde.CachedSerdeFactory

// Cache serde instances for reuse
val factory = CachedSerdeFactory(SerdeFactory())
val userSerde = factory.create<User>()
val orderSerde = factory.create<Order>()
```

## Error Handling

Centurion provides detailed error messages for schema mismatches and data validation:

```kotlin
try {
  // This will fail - wrong number of values
  val invalidStruct = Struct(userSchema, 123L, "John") // Missing email and age
} catch (e: IllegalArgumentException) {
  println("Schema validation error: ${e.message}")
  // Output: Schema size 4 != values size 2
}

try {
  // This will fail - field doesn't exist
  val value = user["nonexistent_field"]
} catch (e: IllegalStateException) {
  println("Field access error: ${e.message}")
}
```

## Schema Evolution

Centurion provides robust support for schema evolution, allowing your data formats to evolve over time without breaking compatibility:

```kotlin
import com.sksamuel.centurion.avro.io.BinaryReader
import com.sksamuel.centurion.avro.io.BinaryWriter
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.decoders.ReflectionRecordDecoder
import org.apache.avro.Schema
import org.apache.avro.SchemaBuilder
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory
import java.io.FileInputStream
import java.io.FileOutputStream

// Original schema
val writerSchema = SchemaBuilder.record("User").fields()
    .requiredString("name")
    .requiredLong("id")
    .endRecord()

// Evolved schema with new field
val readerSchema = SchemaBuilder.record("User").fields()
    .requiredString("name")
    .requiredLong("id")
    .name("email").type(Schema.create(Schema.Type.STRING)).withDefault("")
    .endRecord()

// Old data can be read with new schema
data class UserV1(val name: String, val id: Long)
data class UserV2(val name: String, val id: Long, val email: String)

// Write with old schema
val output = FileOutputStream("user.avro")
val writer = BinaryWriter(
  schema = writerSchema,
  out = output,
  ef = EncoderFactory.get(),
  encoder = ReflectionRecordEncoder(writerSchema, UserV1::class),
  reuse = null
)
writer.write(UserV1("Alice", 123L))
writer.close()

// Read with new schema - email gets default value
val input = FileInputStream("user.avro")
val reader = BinaryReader(
  writerSchema = writerSchema,
  readerSchema = readerSchema,
  input = input,
  factory = DecoderFactory.get(),
  decoder = ReflectionRecordDecoder(readerSchema, UserV2::class),
  reuse = null
)
val user: UserV2 = reader.read() // UserV2("Alice", 123L, "")
```

## Redis Integration (Lettuce)

The `centurion-avro-lettuce` module ships two `io.lettuce.core.codec.RedisCodec`
implementations that let you put Avro-encoded values straight onto a Redis
connection. The codecs cache the underlying `GenericDatumReader`/`Writer`
along with their reflection-derived encoder/decoder, so there is no
per-operation reflection or schema lookup once the codec is constructed.

### Installation

```kotlin
implementation("com.sksamuel.centurion:centurion-avro-lettuce:<version>")
```

### Picking a codec

| Codec                          | Value type      | When to use                                                                       |
|--------------------------------|-----------------|-----------------------------------------------------------------------------------|
| `ReflectionDataClassCodec<T>`  | Kotlin data class | You want to put/get plain data classes; the schema is derived from the class.   |
| `GenericRecordCodec`           | `GenericRecord` | You already work with Avro's generic API or your schema isn't known at compile time. |

Both codecs implement `RedisCodec<T, T>` (same type for keys and values). In
practice you'll pair them with a string key codec via `RedisCodec.of(...)`.

### Caching a data class

```kotlin
import com.sksamuel.centurion.avro.lettuce.ReflectionDataClassCodec
import io.lettuce.core.RedisClient
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

data class User(val id: Long, val name: String, val email: String?)

// Build the codec once and share it for the lifetime of the connection.
val userCodec = ReflectionDataClassCodec(
    encoderFactory = EncoderFactory.get(),
    decoderFactory = DecoderFactory.get(),
    kclass = User::class,
)

val client = RedisClient.create("redis://localhost")
val connection = client.connect(RedisCodec.of(StringCodec.UTF8, userCodec))
val commands = connection.sync()

commands.set("user:123", User(123L, "Alice", "alice@example.com"))
val alice: User = commands.get("user:123")
```

The codec reads the Avro schema from `ReflectionSchemaBuilder` once at
construction time. Reads tolerate buffers that have been positioned or
sliced upstream and never mutate the caller's `ByteBuffer`.

### Caching a generic record

When you already drive Avro through the generic API — for example because
your schema is loaded at runtime — use `GenericRecordCodec`:

```kotlin
import com.sksamuel.centurion.avro.lettuce.GenericRecordCodec
import org.apache.avro.Schema
import org.apache.avro.generic.GenericData

val schema: Schema = Schema.Parser().parse(/* schema JSON or .avsc */)

val recordCodec = GenericRecordCodec(
    schema = schema,
    encoderFactory = EncoderFactory.get(),
    decoderFactory = DecoderFactory.get(),
)

val connection = client.connect(RedisCodec.of(StringCodec.UTF8, recordCodec))
val commands = connection.sync()

val event = GenericData.Record(schema).apply {
    put("id", 1L)
    put("name", "login")
}
commands.set("event:1", event)
```

### Tips

- **One codec per type, share it.** Both codecs are thread-safe and cache
  every reusable component, so build one per type and reuse it across all
  connections.
- **Use the binary Avro format, not data files.** These codecs use Avro's
  schemaless binary encoding (no per-value schema header); both sides of
  the connection must agree on the schema. Use a registry if your
  producers and consumers can drift.
- **Compose with Lettuce's `CompressionCodec`** if you want gzip or LZ4
  on top — wrap the Centurion codec with `CompressionCodec.valueCompressor(...)`.
- **Keys are not Avro.** Keep keys as `StringCodec.UTF8` (or another
  primitive codec). The Centurion codecs are intended for the value half
  of the pair only, even though they implement `RedisCodec<T, T>`.

## Gradle Plugin for Code Generation

Generate Kotlin data classes from Avro schemas at build time:

```kotlin
// build.gradle.kts
plugins {
    id("com.sksamuel.centurion.avro") version "<version>"
}

// The plugin registers three tasks:

// Generate data classes from Avro schemas
tasks.generateDataClasses {
    directory.set("src/main/avro")
}

// Generate optimized encoders
tasks.generateEncoders {
    directory.set("src/main/avro")
}

// Generate optimized decoders
tasks.generateDecoders {
    directory.set("src/main/avro")
}

// Run code generation
./gradlew generateDataClasses generateEncoders generateDecoders
```

## Performance Optimizations

Centurion includes several performance optimizations:

### Reflection Caching
- Uses `LambdaMetafactory` and `MethodHandles` for fast field access
- Caches enum constants mapping
- Optimized primitive type handling

### Resource Pooling
```kotlin
// Reuse binary encoders
val writer = BinaryWriter(schema, output, encoder, reuse = myEncoder)
```

## Performance Tips

- **Reuse readers/writers** when processing multiple files with the same schema
- **Use streaming APIs** for large datasets to avoid loading everything into memory
- **Batch operations** when writing multiple records to improve throughput
- **Enable `globalUseJavaString`** for Avro when working primarily with Java strings
- **Use primitive array types** (`LongArray`, `IntArray`) instead of boxed collections

## When to Use Centurion

Centurion shines in scenarios where you need:

- **High-performance serialization** with minimal overhead for Kotlin/JVM applications
- **Type-safe data persistence** with compile-time guarantees
- **Schema evolution support** for long-lived data formats
- **Integration with big data tools** (Spark, Hadoop, Hive)
- **Redis caching** of complex domain objects

### Comparison with Alternatives

| Feature | Centurion | Protocol Buffers | JSON | Apache Avro (Direct) |
|---------|-----------|------------------|------|---------------------|
| **Kotlin-first API** | ✓ Idiomatic | ✗ Java-style | ✗ Manual parsing | ✗ Java API |
| **Type Safety** | ✓ Compile-time | ✓ Code generation | ✗ Runtime | ✗ Runtime |
| **Schema Evolution** | ✓ Full support | ✓ Limited | ✗ None | ✓ Full support |
| **Performance** | ✓ Optimized | ✓ Fast | ✗ Slower | ✓ Fast |
| **File Size** | ✓ Compact | ✓ Compact | ✗ Larger | ✓ Compact |
| **Human Readable** | ✗ Binary | ✗ Binary | ✓ Yes | ✗ Binary |
| **Big Data Integration** | ✓ Native | ✗ Limited | ✓ Common | ✓ Native |

## Common Issues and Solutions

### Schema Mismatch Errors

```kotlin
// Problem: Field name mismatch
data class User(val username: String) // Schema expects "name"

// Solution: Use @AvroName annotation or match schema exactly
data class User(@AvroName("name") val username: String)
```

### Performance Issues

```kotlin
// Problem: Creating new serde for each operation
fun processUser(user: User) {
    val serde = BinarySerde<User>() // Don't do this repeatedly
    // ...
}

// Solution: Reuse serde instances
class UserService {
    private val serde = BinarySerde<User>() // Create once

    fun processUser(user: User) {
        val bytes = serde.serialize(user)
        // ...
    }
}
```

### Memory Issues with Large Files

```kotlin
// Problem: Loading entire file into memory
val allRecords = reader.readAll() // May cause OOM

// Solution: Use streaming
reader.sequence().forEach { record ->
    // Process one at a time
}
```

## Modules

| Module | Description |
|--------|-------------|
| `centurion-avro` | Avro format support with binary and data file I/O |
| `centurion-avro-lettuce` | Redis integration for Avro serialization |
| `centurion-avro-gradle-plugin` | Gradle plugin for code generation from Avro schemas |

## License

```
This software is licensed under the Apache 2 license, quoted below.

Copyright 2024 Stephen Samuel

Licensed under the Apache License, Version 2.0 (the "License"); you may not
use this file except in compliance with the License. You may obtain a copy of
the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
License for the specific language governing permissions and limitations under
the License.
```
