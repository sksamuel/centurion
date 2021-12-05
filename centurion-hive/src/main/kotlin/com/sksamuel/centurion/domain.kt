package com.sksamuel.centurion

data class DatabaseName(val value: String)
data class TableName(val value: String)
data class FieldName(val value: String)

/**
 * Describes which columns/fields should be partitioned for a particular table.
 */
data class PartitionPlan(val keys: List<PartitionKey>) {
  constructor(vararg keys: PartitionKey) : this(keys.asList())
  companion object {
    val empty = PartitionPlan(emptyList())
  }
}

/**
 * Describes a single key/value pair in a [Partition].
 *
 * Hive nomenclature is that given a table called employees partitioned by city and country,
 * with the following row:
 *
 * { name = "sam", city = "chicago", country = "us" }
 *
 * Then the partition for this row is [ city=chicago, country=us ].
 *
 * In other words, the collection of partition keys and their values is what hive calls a partition.
 */
data class PartitionPart(val field: PartitionKey, val value: String)

data class Partition(val parts: List<PartitionPart>) {
  constructor(vararg parts: PartitionPart) : this(parts.asList())
}

// we only support string partition fields for now
data class PartitionKey(val value: String)

data class PartitionField(val name: String, val type: Type = StringType, val comment: String? = null)
