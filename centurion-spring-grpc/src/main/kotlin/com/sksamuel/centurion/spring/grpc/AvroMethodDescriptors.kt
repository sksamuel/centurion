package com.sksamuel.centurion.spring.grpc

import io.grpc.MethodDescriptor
import kotlin.reflect.KClass

/**
 * Convenience factory methods for building [MethodDescriptor]s whose request and response
 * marshallers are [AvroMarshaller]s backed by centurion-avro.
 */
object AvroMethodDescriptors {

   /**
    * Builds a [MethodDescriptor] for the given [fullMethodName] (e.g. "myservice.MyService/MyMethod")
    * with Avro-backed request and response marshallers.
    */
   inline fun <reified Req : Any, reified Resp : Any> unary(
      fullMethodName: String,
   ): MethodDescriptor<Req, Resp> = unary(fullMethodName, Req::class, Resp::class)

   /**
    * Builds a unary [MethodDescriptor] with Avro-backed marshallers for the given request and
    * response [KClass]es.
    */
   fun <Req : Any, Resp : Any> unary(
      fullMethodName: String,
      requestClass: KClass<Req>,
      responseClass: KClass<Resp>,
   ): MethodDescriptor<Req, Resp> = MethodDescriptor.newBuilder<Req, Resp>()
      .setType(MethodDescriptor.MethodType.UNARY)
      .setFullMethodName(fullMethodName)
      .setRequestMarshaller(AvroMarshaller.forBinary(requestClass))
      .setResponseMarshaller(AvroMarshaller.forBinary(responseClass))
      .build()
}
