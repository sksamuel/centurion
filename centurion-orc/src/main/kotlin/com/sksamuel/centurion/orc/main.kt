//package com.sksamuel.centurion.orc
//
//import org.apache.hadoop.conf.Configuration
//import org.apache.hadoop.fs.Path
//import org.apache.orc.OrcFile
//import org.apache.orc.TypeDescription
//
//fun main() {
//   val conf = Configuration()
//   val schema = TypeDescription.fromString("struct<x:int,y:int>")
//   val writer = OrcFile.createWriter(
//      Path("my-file.orc"),
//      OrcFile.writerOptions(conf)
//         .setSchema(schema)
//   )
//
//   val batch = schema.createRowBatch()
//   val x = batch.cols[0] as LongColumnVector
//   val y = batch.cols[1] as LongColumnVector
//   for (val r = 0; r < 10000; ++r) {
//      val row = batch.size++
//      x.vector[row] = r
//      y.vector[row] = r * 3
//      // If the batch is full, write it out and start over.
//      if (batch.size == batch.getMaxSize()) {
//         writer.addRowBatch(batch);
//         batch.reset();
//      }
//   }
//   if (batch.size != 0) {
//      writer.addRowBatch(batch);
//      batch.reset();
//   }
//   writer.close();
//}
