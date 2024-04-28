package com.sksamuel.centurion.avro.io

import com.sksamuel.centurion.avro.encoders.SpecificRecordEncoder
import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
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
   val encoder = SpecificRecordEncoder(User::class)
   val record = encoder.encode(schema).invoke(user) as GenericRecord

   println(BinaryWriterFactory(schema).writer(ByteArrayOutputStream()).write(record).bytes().size)

   val baos = ByteArrayOutputStream()
   val writer = DataFileWriter(GenericDatumWriter<GenericRecord>(schema))
   writer.setCodec(CodecFactory.deflateCodec(9))
   writer.create(schema, baos)
   writer.append(record)
   writer.flush()
   writer.close()
   println(baos.size())
}
