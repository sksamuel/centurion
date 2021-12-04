package com.sksamuel.centurion.parquet

import java.time.Instant

data class ABTestEvent(
  val deviceId: String,
  val profileId: Long,
  val timestamp: Instant,
)

fun main() {
  val event = ABTestEvent("android", 123, Instant.ofEpochSecond(987))
//  val writer = parquetWriter(Path("Foo"), Configuration(), schema)
}
