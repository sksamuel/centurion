package com.sksamuel.centurion.avro.lettuce

import io.kotest.core.extensions.install
import io.kotest.core.spec.style.FunSpec
import io.kotest.matchers.shouldBe
import io.lettuce.core.codec.RedisCodec
import io.lettuce.core.codec.StringCodec
import kotlinx.coroutines.future.await
import org.apache.avro.io.DecoderFactory
import org.apache.avro.io.EncoderFactory

class ReflectionDataClassCodecTest : FunSpec({

   val redis = install(redisExtension)

   test("happy path") {

      val valueCodec = ReflectionDataClassCodec(
         encoderFactory = EncoderFactory.get(),
         decoderFactory = DecoderFactory.get(),
         Foo::class,
      )

      val keyCodec = StringCodec.UTF8
      val conn = redis.toConnection(codec = RedisCodec.of(keyCodec, valueCodec))

      val foo = Foo("hello", true, 4)
      conn.async().set("foo", foo).await()
      val cached = conn.async().get("foo").await()
      cached shouldBe foo
   }

   test("lists of records") {

      val valueCodec = ReflectionDataClassCodec(
         encoderFactory = EncoderFactory.get(),
         decoderFactory = DecoderFactory.get(),
         Wrapper::class,
      )

      val keyCodec = StringCodec.UTF8
      val conn = redis.toConnection(codec = RedisCodec.of(keyCodec, valueCodec))

      val wrapper = Wrapper(listOf(Obj("hello", true, 4), Obj("world", false, null)))
      conn.async().set("wrapper", wrapper).await()
      val cached = conn.async().get("wrapper").await()
      cached shouldBe wrapper
   }

})

data class Wrapper(val objs: List<Obj>)

data class Obj(
   val a: String,
   val b: Boolean,
   val c: Long?
)
