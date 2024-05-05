package com.sksamuel.centurion.avro.io.serde

private const val DEFAULT_ENCODER_BUFFER_SIZE = 2048
private const val DEFAULT_DECODER_BUFFER_SIZE = 8192
private const val DEFAULT_BLOCK_BUFFER_SIZE = 64 * 1024

data class SerdeOptions(
   val fastReader: Boolean = false,
   val encoderBufferSize: Int = DEFAULT_ENCODER_BUFFER_SIZE,
   val decoderBufferSize: Int = DEFAULT_DECODER_BUFFER_SIZE,
   val blockBufferSize: Int = DEFAULT_BLOCK_BUFFER_SIZE,
)
