package com.sksamuel.reactivehive

import org.apache.hadoop.fs.Path
import java.util.*

/**
 * When creating new files in the Hadoop filestore, an implementation of this
 * interface is used to generate a file name.
 *
 * Users of reactive-hive can supply an implementation of this interface if they
 * wish to control the filename.
 */
interface FileNamer {
  /**
   * @param dir the directory that will contain the file
   * @param partition the partition for the struct that will be written
   *                  can be null if the table is not partitioned
   */
  fun generate(dir: Path, partition: Partition?): String
}

/**
 * Default implementation of [FileNamer] which just generates a random filename, prefixed
 * with the name of this project.
 */
object ReactiveHiveFileNamer : FileNamer {
  override fun generate(dir: Path, partition: Partition?): String =
      "reactivehive_" + UUID.randomUUID().toString().replace("-", "")
}