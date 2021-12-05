package com.sksamuel.centurion

import org.apache.hadoop.fs.Path
import java.util.*

/**
 * When creating new files in the Hadoop filestore, an implementation of this
 * interface is used to generate a file name.
 *
 * Users of rxhive can supply an implementation of this interface if they
 * wish to control the filename.
 */
interface FileNamer {
  /**
   * @param dir the directory that will contain the file
   */
  fun generate(dir: Path): String
}

/**
 * Default implementation of [FileNamer] which just generates a random filename, prefixed
 * with the name of this project.
 */
object RxHiveFileNamer : FileNamer {
  override fun generate(dir: Path): String =
      "rxhive_" + UUID.randomUUID().toString().replace("-", "")
}

object UUIDFileNamer : FileNamer {
  override fun generate(dir: Path): String = UUID.randomUUID().toString()
}

/**
 * Useful for testing, this implementation of [FileNamer] always returns
 * the same file name as supplied in the constructor.
 */
class ConstantFileNamer(val name: String) : FileNamer {
  override fun generate(dir: Path): String = name
}
