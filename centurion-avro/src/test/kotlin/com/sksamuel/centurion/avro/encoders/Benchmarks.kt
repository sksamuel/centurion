package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.generation.ReflectionSchemaBuilder
import org.apache.avro.generic.GenericData
import org.apache.avro.util.Utf8
import kotlin.random.Random
import kotlin.time.measureTime

data class User(
   val userId: Long,
   val name: String?,
   val email: String?,
   val lastActiveTimestamp: Long,
   val type: UserType?,
   val location: String,
   val age: Int,
   val height: Int,
   val weight: Int,
)

enum class UserType { User, Admin }

fun main() {

   val user = User(
      userId = Random.nextLong(),
      name = "sammy mcsamface",
      email = "sammy@mcsamface.com",
      lastActiveTimestamp = Random.nextLong(),
      type = UserType.Admin,
      location = "Chicago",
      age = 45,
      height = 180,
      weight = 200,
   )
   val schema = ReflectionSchemaBuilder(true).schema(User::class)

   GenericData.setStringType(schema, GenericData.StringType.String)

   val sets = 10
   val reps = 5_000_000

   repeat(sets) {
      val encoder = ReflectionRecordEncoder()
      val time = measureTime {
         repeat(reps) {
            encoder.encode(schema, user)
         }
      }
      println("ReflectionRecordEncoder: $time")
   }

   repeat(sets) {
      val encoder = SpecificRecordEncoder(User::class)
      val time = measureTime {
         repeat(reps) {
            encoder.encode(schema, user)
         }
      }
      println("SpecificRecordEncoder globalUseJavaString=false: $time")
   }

   repeat(sets) {
      Encoder.globalUseJavaString = true
      val encoder = SpecificRecordEncoder(User::class)
      val time = measureTime {
         repeat(reps) {
            encoder.encode(schema, user)
         }
      }
      println("SpecificRecordEncoder globalUseJavaString=true: $time")
   }

   repeat(sets) {

      val encoder = Encoder<User> { schema, value ->
         val record = GenericData.Record(schema)
         record.put("userId", value.userId)
         record.put("name", Utf8(value.name))
         record.put("email", Utf8(value.email))
         record.put("lastActiveTimestamp", value.lastActiveTimestamp)
         record.put("type", GenericData.get().createEnum("Admin", schema.getField("type").schema()))
         record.put("location", Utf8(value.location))
         record.put("age", value.age)
         record.put("height", value.height)
         record.put("weight", value.weight)
         record
      }

      val time = measureTime {
         repeat(reps) {
            encoder.encode(schema, user)
         }
      }
      println("CustomEncoder: $time")
   }
}
