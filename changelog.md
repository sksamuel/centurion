## Changelog

### 1.2.1

* Added optimized string and primitive decoders
* Added `Instant`, `LocalTime`, `LocalDateTime`, `OffsetDateTime` encoders and decoders
* Added `JavaStringUUIDEncoder`
* Added `RecordEncoderGenerator`
* Optimized pattern matching for primitive decoders
* Optimized field lookups for reflection based encoders

### 1.2.0 (April 2024)

* Renamed AvroBinaryWriter to BinaryWriter, and AvroBinaryReader to BinaryReader
* Added convenience functions to BinaryWriter and BinaryReader for single object operations.
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
