## Changelog

### 1.4.0 (April 2025)

* Use LambdaMetafactory and MethodHandles for much improved reflection performance in encoding and decoding records.
* Simplified the Encoder and Decoder interfaces
* Optimized encoding and decoding for Long and Integer sets, longs and arrays.
* Removed obsolete strict decoding option
* Decoder.map changed to be an extension function.

### 1.3.3 (May 2024)

* Added `SpecificSerde` and `CompressingSerde`.
* Added `ReflectionSerdeFactory` and `CachedReflectionSerdeFactory`.
* Added `DataReader` and `DataWriter`.

### 1.3.2 (April 2024)

* Optimized `StringEncoder` when `globalUseJavaString` is set
* Added `LongArray` and `IntArray` support.

### 1.3.1 (April 2024)

* Fixed record generators to only use schema access at creation time.

### 1.3.0 (April 2024)

* Encoder and Decoder contract changed to return a function which accepts a schema, in order to pre-compute field access.

### 1.2.1 (April 2024)

* Added optimized string and primitive decoders
* Added `Instant`, `LocalTime`, `LocalDateTime`, `OffsetDateTime` encoders and decoders
* Added `JavaStringUUIDEncoder`
* Added `RecordEncoderGenerator`
* Optimized pattern matching for primitive decoders
* Optimized field lookups for reflection based encoders

### 1.2.0 (April 2024)

* Renamed `AvroBinaryWriter to `BinaryWriter`, and `AvroBinaryReader` to `BinaryReader`
* Added convenience functions to `BinaryWriter and `BinaryReader` for single object operations.
* Added `Serde` class for convenient reflection based serialization and deserialization
* Added `CachedSpecificRecordEncoder` to cache lazily created [SpecificRecordEncoder] instances.
* Support strings and utf8 when decoding enums

### 1.1.3 (April 2024)

* Map decoder should support UTF8 strings

### 1.1.2 (April 2024)

* Added `Encoder.globalUseJavaString` to  to `centurion-avro` to prefer Strings over Utf8 in all cases.
* Optimized Avro array creation

### 1.1.1 (April 2024)

* Added Map encoding support to `centurion-avro`

### 1.1.0 (April 2024)

* Added [Avro4s](https://github.com/sksamuel/avro4s) port of Encoders, Decoder, Writers and Readers to `centurion-avro`
