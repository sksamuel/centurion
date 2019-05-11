package com.sksamuel.reactivehive.parquet.converters

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class DictionaryStringPrimitiveConverter(private val receiver: Receiver<Any>) : PrimitiveConverter() {

  private var dictionary: Dictionary? = null

  override fun hasDictionarySupport(): Boolean = true

  override fun setDictionary(dictionary: Dictionary) {
    this.dictionary = dictionary
  }

  override fun addValueFromDictionary(dictionaryId: Int) {
    addBinary(dictionary!!.decodeToBinary(dictionaryId))
  }

  override fun addBinary(x: Binary) {
    receiver.add(x.toStringUsingUTF8())
  }
}