package com.sksamuel.centurion.spring.grpc

import io.grpc.MethodDescriptor
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe

data class Person(val name: String, val age: Int, val nickname: String?)

class AvroMarshallerTest : FunSpec({

   test("round trip via stream / parse") {
      val marshaller = AvroMarshaller.forBinary<Person>()
      val original = Person("Ada", 36, "Augusta")
      val parsed = marshaller.parse(marshaller.stream(original))
      parsed shouldBe original
   }

   test("round trip with null field") {
      val marshaller = AvroMarshaller.forBinary<Person>()
      val original = Person("Ada", 36, null)
      val parsed = marshaller.parse(marshaller.stream(original))
      parsed shouldBe original
   }

   test("reuses underlying serde across calls") {
      val marshaller = AvroMarshaller.forBinary<Person>()
      val people = listOf(
         Person("Ada", 36, "Augusta"),
         Person("Grace", 85, null),
         Person("Linus", 56, "Linus"),
      )
      people.forEach { p ->
         marshaller.parse(marshaller.stream(p)) shouldBe p
      }
   }

   test("AvroMethodDescriptors builds a unary descriptor with Avro marshallers") {
      val descriptor: MethodDescriptor<Person, Person> =
         AvroMethodDescriptors.unary("people.PeopleService/Echo")
      descriptor.fullMethodName shouldBe "people.PeopleService/Echo"
      descriptor.type shouldBe MethodDescriptor.MethodType.UNARY
      val original = Person("Ada", 36, null)
      descriptor.parseRequest(descriptor.streamRequest(original)) shouldBe original
      descriptor.parseResponse(descriptor.streamResponse(original)) shouldBe original
   }
})
