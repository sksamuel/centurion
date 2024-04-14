# Centurion <img src="logo.png" height="50">

![master](https://github.com/sksamuel/centurion/workflows/master/badge.svg)
[<img src="https://img.shields.io/maven-central/v/com.sksamuel.centurion/centurion-schemas.svg?label=latest%20release"/>](http://search.maven.org/#search%7Cga%7C1%7Ccenturion)
[<img src="https://img.shields.io/nexus/s/https/s01.oss.sonatype.org/com.sksamuel.centurion/centurion-avro.svg?label=latest%20snapshot&style=plastic"/>](https://oss.sonatype.org/content/repositories/snapshots/com/sksamuel/centurion/)
![License](https://img.shields.io/github/license/sksamuel/centurion.svg?style=plastic)

## Introduction

Centurion is a JVM (written in Kotlin) toolkit for columnar and streaming formats.

This library allows you to read, write and convert between the following formats:

* [Apache Parquet](https://parquet.apache.org)
* [Apache Orc](https://orc.apache.org)
* [Apache Arrow IPC](https://arrow.apache.org)
* [Apache Avro](https://avro.apache.org)

## Schema Conversions

Centurion allows easy conversion of schemas between any of the supported formats, via Centurion's own internal format.

This internal format is a superset of the functionality of all the supported formats, and is intended as an intermediate
format only to allow for conversions.

The following table shows how types map between each of the formats.

| Centurion Type  | Avro                                     | Parquet                   | Orc         | Arrow               |
|-----------------|------------------------------------------|---------------------------|-------------|---------------------|
| Strings         | String                                   | Binary (String)           | String      | Utf8                |
| UUID            | String (UUID)                            | Binary (String)           | String      | Utf8                |
| Booleans        | Boolean                                  | Boolean                   | Boolean     | Bool                |
| Int64           | Long                                     | Int64                     | Long        | Int64 Signed        |
| Int32           | Int                                      | Int32                     | Int         | Int32 Signed        |
| Int16           | N/A (Int)                                | Int32 (Signed Int16)      | Short       | Int16 Signed        |
| Int8            | N/A (Int)                                | Int32 (Signed Int8)       | Byte        | Int8 Signed         |
| Float64         | Double                                   | Double                    | Double      | FloatingPointDouble |
| Float32         | Float                                    | Float                     | Float       | FloatingPointSingle |
| Enum            | Enum                                     | Enum                      | String      | String              |
| Decimal         | Binary / Fixed with annotation _Decimal_ | Decimal(precision, scale) | Decimal)    | Decimal             |
| Varchar         | Fixed)                                   | N/A (String)              | Varchar     | N/A (String)        |
| TimestampMillis | Long (TimestampMillis)                   | Int64 (Timestamp)         | Timestamp   | Timestamp (Millis)  |
| TimestampMicros | Long (TimestampMicros)                   | Int64 (Timestamp)         | Unsupported | Timestamp (Micros)  |
| Map             | Map                                      | Map                       | Map         | Map                 |
