package com.sksamuel.reactivehive

interface Type

data class Struct(val schema: StructType, val values: List<Any>) : Type

object BooleanType : Type
object StringType : Type
object BinaryType : Type
object DoubleType : Type
object FloatType : Type
object ByteType : Type
object IntType : Type
object LongType : Type
object ShortType : Type

data class ArrayType(val elementType: Type) : Type