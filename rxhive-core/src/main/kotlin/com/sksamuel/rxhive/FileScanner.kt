package com.sksamuel.rxhive

import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path

/**
 * When reading data from Hive, there may be multiple files per partition or table.
 * Therefore we need to scan the file system to pick these up.
 * Implementations of this interface will handle the scanning of a directory.
 */
interface FileScanner {
  fun scan(path: Path, fs: FileSystem): List<Path>
}

/**
 * Default implementation of [FileScanner] that ignores "hidden" files.
 * Hive usually treats files beginning with a dot as hidden, and this file scanner does the same.
 * All remaining non empty files are returned.
 * This scanner does recursive into other directories.
 */
object DefaultFileScanner : FileScanner {

  override fun scan(path: Path, fs: FileSystem): List<Path> {
    val iter = fs.listFiles(path, false)
    return RemoteIter(iter).asSequence()
        .filterNot { it.path.name.startsWith(".") }
        .filterNot { it.len == 0L }
        .map { it.path }.toList()
  }
}