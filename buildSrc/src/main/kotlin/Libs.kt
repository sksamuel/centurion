object Libs {

  const val kotlinVersion = "1.6.0"
  const val org = "com.sksamuel.hoplite"

  object Kotlin {
    const val reflect = "org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion"
    const val stdlib = "org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion"
    const val datetime = "org.jetbrains.kotlinx:kotlinx-datetime:0.2.0"
    const val coroutines = "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0-RC"
  }

  object Aws {
    private const val version = "1.12.36"
    const val core = "com.amazonaws:aws-java-sdk-core:$version"
    const val ssm = "com.amazonaws:aws-java-sdk-ssm:$version"
    const val secrets = "com.amazonaws:aws-java-sdk-secretsmanager:$version"
  }

  object CronUtils {
    const val utils = "com.cronutils:cron-utils:9.1.3"
  }

  object Hadoop {
    const val common = "org.apache.hadoop:hadoop-common:2.10.1"
  }

  object Jackson {
    const val core = "com.fasterxml.jackson.core:jackson-core:2.12.3"
    const val databind = "com.fasterxml.jackson.core:jackson-databind:2.12.3"
  }

  object Kotest {
    private const val version = "5.0.1"
    const val assertions = "io.kotest:kotest-assertions-core:$version"
    const val junit5 = "io.kotest:kotest-runner-junit5:$version"
  }
}
