package com.sksamuel.centurion.avro.encoders

import com.sksamuel.centurion.avro.Encoder

val ByteEncoder: Encoder<Byte> = Encoder.identity()
val ShortEncoder: Encoder<Short> = Encoder.identity()
val IntEncoder: Encoder<Int> = Encoder.identity()
val LongEncoder: Encoder<Long> = Encoder.identity()
val BooleanEncoder: Encoder<Boolean> = Encoder.identity()
val FloatEncoder: Encoder<Float> = Encoder.identity()
val DoubleEncoder: Encoder<Double> = Encoder.identity()
