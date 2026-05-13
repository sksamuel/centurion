package com.sksamuel.centurion.ktor

import com.sksamuel.centurion.avro.io.serde.BinarySerdeFactory
import com.sksamuel.centurion.avro.io.serde.CachingSerdeFactory
import com.sksamuel.centurion.avro.io.serde.Serde
import com.sksamuel.centurion.avro.io.serde.SerdeFactory
import io.ktor.http.ContentType
import io.ktor.http.content.ByteArrayContent
import io.ktor.http.content.OutgoingContent
import io.ktor.serialization.ContentConverter
import io.ktor.util.reflect.TypeInfo
import io.ktor.utils.io.ByteReadChannel
import io.ktor.utils.io.readRemaining
import kotlinx.io.readByteArray
import kotlin.reflect.KClass
import kotlin.text.Charsets

/**
 * The default `application/avro` media type used by [AvroContentConverter] when no
 * explicit content type is supplied at registration time.
 */
val AvroContentType: ContentType = ContentType("application", "avro")

/**
 * A Ktor [ContentConverter] that serializes and deserializes request / response bodies
 * using centurion-avro's [Serde] interface.
 *
 * The same converter implementation is suitable for both the server-side and client-side
 * Content Negotiation plugins — Ktor's [ContentConverter] SPI is shared between the two,
 * so a single registration call covers both directions of traffic.
 *
 * Internally the converter resolves a [Serde] per type via the supplied [SerdeFactory].
 * The default factory caches serdes by [KClass], so reflection-based schema/encoder/decoder
 * construction happens at most once per type.
 */
class AvroContentConverter(
   private val serdeFactory: SerdeFactory = CachingSerdeFactory(BinarySerdeFactory()),
) : ContentConverter {

   override suspend fun serialize(
      contentType: ContentType,
      charset: java.nio.charset.Charset,
      typeInfo: TypeInfo,
      value: Any?,
   ): OutgoingContent? {
      if (value == null) return null
      val serde = serdeFor(typeInfo)
      val bytes = serde.serialize(value)
      return ByteArrayContent(bytes, contentType)
   }

   override suspend fun deserialize(
      charset: java.nio.charset.Charset,
      typeInfo: TypeInfo,
      content: ByteReadChannel,
   ): Any? {
      val bytes = content.readRemaining().readByteArray()
      val serde = serdeFor(typeInfo)
      return serde.deserialize(bytes)
   }

   @Suppress("UNCHECKED_CAST")
   private fun serdeFor(typeInfo: TypeInfo): Serde<Any> {
      val kclass = typeInfo.type as KClass<Any>
      return serdeFactory.serdeFor(kclass)
   }
}

/**
 * Register [AvroContentConverter] for the given [contentType] on the receiver Ktor
 * Content Negotiation configuration. Works for both the server- and client-side
 * Content Negotiation plugins because both share the same [io.ktor.serialization.Configuration]
 * SPI.
 *
 * Example (server):
 * ```
 * install(ContentNegotiation) {
 *    avro()
 * }
 * ```
 *
 * Example (client):
 * ```
 * val client = HttpClient {
 *    install(ContentNegotiation) {
 *       avro()
 *    }
 * }
 * ```
 */
fun io.ktor.serialization.Configuration.avro(
   contentType: ContentType = AvroContentType,
   serdeFactory: SerdeFactory = CachingSerdeFactory(BinarySerdeFactory()),
) {
   register(contentType, AvroContentConverter(serdeFactory))
}
