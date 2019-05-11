package com.sksamuel.reactivehive

data class DatabaseName(val value: String)
data class TableName(val value: String)

data class PartitionPlan(val keys: List<PartitionKey>)
data class PartitionKey(val value: String)
data class PartitionField(val name: String, val type: Type = StringType, val comment: String? = null)
data class Partition(val entries: List<Pair<PartitionKey, String>>) {
  constructor(vararg entries: Pair<PartitionKey, String>) : this(entries.asList())
}