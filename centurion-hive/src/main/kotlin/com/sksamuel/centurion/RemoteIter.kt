package com.sksamuel.centurion

import org.apache.hadoop.fs.RemoteIterator

class RemoteIter<T>(val iter: RemoteIterator<T>) : Iterator<T> {
  override fun next(): T = iter.next()
  override fun hasNext(): Boolean = iter.hasNext()
}
