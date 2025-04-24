package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schemas.ReflectionSchemaBuilder
import org.apache.avro.file.CodecFactory
import org.apache.avro.file.DataFileWriter
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import java.io.ByteArrayOutputStream
import kotlin.random.Random

data class User(
   val userId: Long,
   val name: String?,
   val email: String?,
   val lastActiveTimestamp: Long,
   val type: UserType?,
)

enum class UserType { User, Admin }

fun main() {

   val user = User(Random.nextLong(), "sammy mcsamface", "sammy@mcsamface.com", Random.nextLong(), UserType.Admin)
   val schema = ReflectionSchemaBuilder(true).schema(User::class)

   GenericData.setStringType(schema, GenericData.StringType.String)
   val encoder = ReflectionRecordEncoder()
   val record = encoder.encode(schema, user) as GenericRecord

   val baos1 = ByteArrayOutputStream()
   val writer1 = DataFileWriter(GenericDatumWriter<GenericRecord>(schema))
   writer1.create(schema, baos1)
   writer1.append(record)
   writer1.flush()
   writer1.close()
   println("data size: ".padEnd(40) + baos1.size())

   val baos2 = ByteArrayOutputStream()
   val writer2 = DataFileWriter(GenericDatumWriter<GenericRecord>(schema))
   writer2.setCodec(CodecFactory.deflateCodec(9))
   writer2.create(schema, baos2)
   writer2.append(record)
   writer2.flush()
   writer2.close()
   println("deflateCodec(9) size: ".padEnd(40) + baos2.size())
}
