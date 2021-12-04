package com.sksamuel.centurion.parquet.converters

import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class DictionaryStringPrimitiveConverter(
  private val index: Int,
  private val collector: ValuesCollector,
) : PrimitiveConverter() {

  private var dictionary: Dictionary? = null

  override fun hasDictionarySupport(): Boolean = true

  override fun setDictionary(dictionary: Dictionary) {
    this.dictionary = dictionary
  }

  override fun addValueFromDictionary(dictionaryId: Int) {
    addBinary(dictionary!!.decodeToBinary(dictionaryId))
  }

  override fun addBinary(x: Binary) {
    collector[index] = x.toStringUsingUTF8()
  }
}
