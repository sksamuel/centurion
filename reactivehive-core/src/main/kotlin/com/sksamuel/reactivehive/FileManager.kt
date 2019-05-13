package com.sksamuel.reactivehive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * Implementations of this interface are responsible for the process by
 * which files are prepared and committed.
 *
 * This is a two step process:
 *
 * 1. Step one - prepare - must return a Path to which data will be written.
 * 2. Step two - complete - all data has been written to the file
 */
interface FileManager {
  fun prepare(dir: Path, fs: FileSystem): Path
  fun complete(path: Path, fs: FileSystem): Path
}

/**
 * An implementation of [FileManager] which creates invisible files for writing,
 *  and then makes them visible once the file is completed.
 */
class StagingFileManager(val namer: FileNamer = ReactiveHiveFileNamer) : FileManager {

  override fun prepare(dir: Path, fs: FileSystem): Path {
    val filename = namer.generate(dir)
    val path = Path(dir, ".$filename")
    fs.delete(path, false)
    return path
  }

  override fun complete(path: Path, fs: FileSystem): Path {
    val finalPath = Path(path.parent, path.name.removePrefix("."))
    fs.rename(path, finalPath)
    return finalPath
  }
}

class OptimisticFileManager(val namer: FileNamer) : FileManager {

  override fun prepare(dir: Path, fs: FileSystem): Path {
    val filename = namer.generate(dir)
    val path = Path(dir, filename)
    fs.delete(path, false)
    return path
  }

  override fun complete(path: Path, fs: FileSystem): Path = path
}