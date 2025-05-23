package com.sksamuel.centurion.avro.io

import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.github.luben.zstd.ZstdOutputStream
import com.ning.compress.lzf.LZFOutputStream
import com.sksamuel.centurion.avro.Foo
import com.sksamuel.centurion.avro.createFoo
import com.sksamuel.centurion.avro.encoders.ReflectionRecordEncoder
import com.sksamuel.centurion.avro.schema
import org.apache.avro.generic.GenericData
import org.apache.avro.generic.GenericDatumWriter
import org.apache.avro.generic.GenericRecord
import org.apache.avro.io.EncoderFactory
import org.xerial.snappy.SnappyOutputStream
import java.io.ByteArrayOutputStream
import java.util.zip.DeflaterOutputStream
import java.util.zip.GZIPOutputStream

fun main() {
   GenericData.setStringType(schema, GenericData.StringType.String)

   val foo = createFoo()
   val mapper = jacksonObjectMapper()

   uncompressed("Jackson") { mapper.writeValueAsBytes(foo) }
   runGzip("Jackson") { mapper.writeValueAsBytes(foo) }
   runDeflate("Jackson") { mapper.writeValueAsBytes(foo) }
   runSnappy("Jackson") { mapper.writeValueAsBytes(foo) }
   runLZF("Jackson") { mapper.writeValueAsBytes(foo) }
   zstd("Jackson") { mapper.writeValueAsBytes(foo) }

   uncompressed("Avro") { toAvroByteArray(foo) }
   runGzip("Avro") { toAvroByteArray(foo) }
   runDeflate("Avro") { toAvroByteArray(foo) }
   runSnappy("Avro") { toAvroByteArray(foo) }
   runLZF("Avro") { toAvroByteArray(foo) }
   zstd("Avro") { toAvroByteArray(foo) }
}

fun toAvroByteArray(foo: Foo): ByteArray {

   val encoder = ReflectionRecordEncoder<Foo>(schema)
   val record = encoder.encode(schema, foo) as GenericRecord

   val baos = ByteArrayOutputStream()
   val binaryEncoder = EncoderFactory.get().binaryEncoder(baos, null)

   val datum = GenericDatumWriter<GenericRecord>(schema)
   datum.write(record, binaryEncoder)
   binaryEncoder.flush()
   return baos.toByteArray()
}

fun runGzip(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   val output = GZIPOutputStream(baos)
   output.write(f())
   output.close()
   val size = baos.toByteArray().size
   println("$name (GZIPOutputStream)".padEnd(50) + " ${size}b")
}

fun runDeflate(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   val output = DeflaterOutputStream(baos)
   output.write(f())
   output.close()
   val size = baos.toByteArray().size
   println("$name (DeflaterOutputStream)".padEnd(50) + " ${size}b")
}

fun runSnappy(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   val output = SnappyOutputStream(baos)
   output.write(f())
   output.close()
   val size = baos.toByteArray().size
   println("$name (SnappyOutputStream)".padEnd(50) + " ${size}b")
}

fun runLZF(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   val output = LZFOutputStream(baos)
   output.write(f())
   output.close()
   val size = baos.toByteArray().size
   println("$name (LZFOutputStream)".padEnd(50) + " ${size}b")
}

fun zstd(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   val output = ZstdOutputStream(baos)
   output.write(f())
   output.close()
   val size = baos.toByteArray().size
   println("$name (ZstdOutputStream)".padEnd(50) + " ${size}b")
}

fun uncompressed(name: String, f: () -> ByteArray) {
   val baos = ByteArrayOutputStream()
   baos.write(f())
   baos.close()
   val size = baos.toByteArray().size
   println("$name (Uncompressed)".padEnd(50) + " ${size}b")
}
