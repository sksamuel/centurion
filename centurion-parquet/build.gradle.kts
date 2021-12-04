dependencies {
   api(project(Projects.schemas))
   implementation(Libs.Parquet.common)
   implementation(Libs.Parquet.hadoop)
   implementation(Libs.Hadoop.common)
}

apply("../publish.gradle.kts")
