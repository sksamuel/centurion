package com.sksamuel.centurion.avro.io

/**
 * The [Format] enum describes the three types of encoding avro supports when reading and writing.
 *
 * - [Data] will write records as binary, and includes the schema in the output.
 *          This format results in larger sizes than [Binary], clearly as the schema takes up room, but supports
 *          schema evolution, as the deserializers can compare expected schema with the written schema.
 *
 * - [Binary] will write records as binary, but omits the schema from the output.
 *            This format results in the smallest sizes, and is similar to how protobuf works. However, readers
 *            are required to know the schema when deserializing.
 *
 * - [Json] will write records as json, including the schema.
 */
enum class Format {
   Binary,
   Data,
   Json
}
