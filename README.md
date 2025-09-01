# Centurion <img src="logo.png" height="50">

![master](https://github.com/sksamuel/centurion/workflows/master/badge.svg)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.centurion/centurion-avro.svg?label=latest%20release"/>](https://central.sonatype.com/artifact/com.sksamuel.centurion/centurion-avro)
[<img src="https://img.shields.io/maven-metadata/v?metadataUrl=https%3A%2F%2Fcentral.sonatype.com%2Frepository%2Fmaven-snapshots%2Fcom%2Fsksamuel%2Fcenturion%2Fcenturion-avro%2Fmaven-metadata.xml&strategy=highestVersion&label=maven-snapshot">](https://central.sonatype.com/repository/maven-snapshots/com/sksamuel/centurion/centurion-avro/maven-metadata.xml)
![License](https://img.shields.io/github/license/sksamuel/centurion.svg?style=plastic)

Centurion is a Kotlin toolkit for columnar and streaming data formats. It provides convenient methods to read, write and convert between the following formats:

* **[Apache Avro](https://avro.apache.org)** - Data serialization system with rich schema evolution support
* **[Apache Parquet](https://parquet.apache.org)** - Columnar storage format optimized for analytics
* **[Apache Orc](https://orc.apache.org)** - High-performance columnar format with advanced compression

See [changelog](changelog.md) for release notes.

## Features

- **Type-safe schema definitions:** Define schemas using Kotlin's type system with compile-time safety
- **Multiple format support:** Seamlessly work with Avro, Parquet, and ORC formats
- **Streaming operations:** Efficient streaming readers and writers for large datasets
- **Schema conversion:** Convert schemas between different formats
- **Kotlin-first API:** Idiomatic Kotlin APIs with coroutines support where applicable
- **High performance:** Built on top of proven Apache libraries with optimizations for Kotlin usage

## Getting Started

Add Centurion to your build depending on which formats you need:

```kotlin
// For Avro support
implementation("com.sksamuel.centurion:centurion-avro:<version>")

// For Parquet support
implementation("com.sksamuel.centurion:centurion-parquet:<version>")

// For ORC support
implementation("com.sksamuel.centurion:centurion-orc:<version>")
```

## Avro Operations

### Writing Avro Data

```kotlin
import com.sksamuel.centurion.avro.io.BinaryWriter
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
  val writer = BinaryWriter(schema, output, encoder)
  records.forEach { writer.write(it) }
  writer.close()
}
```

### Reading Avro Data

```kotlin
import com.sksamuel.centurion.avro.io.BinaryReader
import java.io.FileInputStream

// Read from Avro binary format
FileInputStream("users.avro").use { input ->
  val reader = BinaryReader(writerSchema, readerSchema, input, factory, decoder)
  val records = reader.readAll()
  records.forEach { struct ->
    println("User: ${struct["name"]}, ID: ${struct["id"]}")
  }
}
```

## Parquet Operations

### Writing Parquet Data

```kotlin
import com.sksamuel.centurion.parquet.Parquet
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

// Define schema and data
val schema = Schema.Struct(
  Schema.Field("product_id", Schema.Strings),
  Schema.Field("quantity", Schema.Int32),
  Schema.Field("price", Schema.Decimal(Schema.Precision(10), Schema.Scale(2)))
)

val data = listOf(
  Struct(schema, "PROD-001", 10, java.math.BigDecimal("29.99")),
  Struct(schema, "PROD-002", 5, java.math.BigDecimal("15.50")),
  Struct(schema, "PROD-003", 20, java.math.BigDecimal("8.75"))
)

// Write to Parquet
val path = Path("sales.parquet")
val conf = Configuration()
val writer = Parquet.writer(path, schema, conf)

data.forEach { struct ->
  writer.write(struct)
}
writer.close()
```

### Reading Parquet Data

```kotlin
import com.sksamuel.centurion.parquet.Parquet

// Read from Parquet
val path = Path("sales.parquet")
val conf = Configuration()
val reader = Parquet.reader(path, conf)

var struct = reader.read()
while (struct != null) {
  println("Product: ${struct["product_id"]}, Qty: ${struct["quantity"]}")
  struct = reader.read()
}
reader.close()

// Count records efficiently
val recordCount = Parquet.count(listOf(path), conf)
println("Total records: $recordCount")
```

## Schema Conversion

Convert between different format schemas:

```kotlin
import com.sksamuel.centurion.avro.schemas.toAvroSchema
import com.sksamuel.centurion.parquet.schemas.ToParquetSchema

// Convert Centurion schema to Avro schema
val centurionSchema = Schema.Struct(
  Schema.Field("name", Schema.Strings),
  Schema.Field("age", Schema.Int32)
)

val avroSchema = centurionSchema.toAvroSchema()

// Convert to Parquet schema
val parquetSchema = ToParquetSchema.toParquetType(centurionSchema)
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

## Performance Tips

- **Reuse readers/writers** when processing multiple files with the same schema
- **Use streaming APIs** for large datasets to avoid loading everything into memory
- **Choose appropriate compression** for Parquet files based on your data characteristics
- **Batch operations** when writing multiple records to improve throughput

## Modules

| Module | Description |
|--------|-------------|
| `centurion-schemas` | Core schema definitions and Struct implementations |
| `centurion-avro` | Avro format support with binary and data file I/O |
| `centurion-parquet` | Parquet format support with Hadoop integration |
| `centurion-orc` | ORC format support for high-performance analytics |
| `centurion-avro-lettuce` | Redis integration for Avro serialization |

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
