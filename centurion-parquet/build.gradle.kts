dependencies {
   api(project(Projects.schemas))
   implementation(Libs.Parquet.common)
   implementation(Libs.Parquet.hadoop)
   api(Libs.Hadoop.common)
   implementation(Libs.Hadoop.client)
   testImplementation("org.apache.spark:spark-core_2.12:3.2.0")
   testImplementation("org.apache.spark:spark-sql_2.12:3.2.0")
}

apply("../publish.gradle.kts")
