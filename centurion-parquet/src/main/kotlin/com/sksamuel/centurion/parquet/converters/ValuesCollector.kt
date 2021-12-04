package com.sksamuel.centurion.parquet.converters

interface ValuesCollector {

  /**
   * Add an element to the end of the values.
   * Used when repeated values, and we don't know the size in advance.
   */
  fun add(value: Any?)

  /**
   * Add an element at a specific point.
   * Used when building structs, and we want to insert at a known location.
   */
  operator fun set(pos: Int, value: Any?)

  fun values(): List<Any?>

  fun reset()
}

class RepeatedValuesCollector : ValuesCollector {

  private val values = mutableListOf<Any?>()

  override fun add(value: Any?) {
    values.add(value)
  }

  override fun reset() {
    values.clear()
  }

  override fun values(): List<Any?> {
    return values.toList()
  }

  override fun set(pos: Int, value: Any?) {
    values.add(value)
  }
}

class StructValuesCollector(private val size: Int) : ValuesCollector {

  private var values = Array<Any?>(size) { null }

  /**
   * Add an element to the end of the values.
   * Used when building maps or lists.
   */
  override fun add(value: Any?) {
    error("Unsupported operation on StructValuesCollector")
  }

  /**
   * Add an element at a specific point.
   * Used when building structs.
   */
  override fun set(pos: Int, value: Any?) {
    require(pos < size) { "Index $pos should be less than size of array $size" }
    values[pos] = value
  }

  override fun values(): List<Any?> = values.toList()

  override fun reset() {
    values = Array<Any?>(size) { null }
  }
}
