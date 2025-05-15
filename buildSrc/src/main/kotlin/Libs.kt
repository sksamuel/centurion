object Libs {

   const val kotlinVersion = "1.8.21"
   const val org = "com.sksamuel.centurion"

   object Kotlin {
      const val reflect = "org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion"
      const val stdlib = "org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion"
      const val datetime = "org.jetbrains.kotlinx:kotlinx-datetime:0.2.0"
      const val coroutines = "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0-RC"
   }

   object Parquet {
      const val common = "org.apache.parquet:parquet-common:1.11.2"
      const val hadoop = "org.apache.parquet:parquet-hadoop:1.11.2"
   }

   object Orc {
      const val core = "org.apache.orc:orc-core:2.1.2"
   }

   object Hadoop {
      const val common = "org.apache.hadoop:hadoop-common:2.10.1"
      const val client = "org.apache.hadoop:hadoop-client:2.10.1"
   }
}

object Projects {
   const val schemas = ":centurion-schemas"
}
