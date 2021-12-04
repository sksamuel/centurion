package com.sksamuel.centurion.parquet.converters

import com.sksamuel.centurion.StructBuilder
import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class DictionaryStringPrimitiveConverter(
  private val fieldName: String,
  private val builder: StructBuilder
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
    builder[fieldName] = x.toStringUsingUTF8()
  }
}
