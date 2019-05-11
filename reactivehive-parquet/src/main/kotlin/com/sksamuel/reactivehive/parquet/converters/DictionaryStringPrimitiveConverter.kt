package com.sksamuel.reactivehive.parquet.converters

import com.sksamuel.reactivehive.StructField
import org.apache.parquet.column.Dictionary
import org.apache.parquet.io.api.Binary
import org.apache.parquet.io.api.PrimitiveConverter

class DictionaryStringPrimitiveConverter(private val field: StructField,
                                         private val builder: MutableMap<String, Any?>) : PrimitiveConverter() {

  private var dictionary: Dictionary? = null

  override fun hasDictionarySupport(): Boolean = true

  override fun setDictionary(dictionary: Dictionary) {
    this.dictionary = dictionary
  }

  override fun addValueFromDictionary(dictionaryId: Int) {
    addBinary(dictionary!!.decodeToBinary(dictionaryId))
  }

  override fun addBinary(x: Binary) {
    builder[field.name] = x.toStringUsingUTF8()
  }
}