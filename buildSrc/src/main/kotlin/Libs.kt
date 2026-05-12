object Libs {

   const val kotlinVersion = "1.8.21"
   const val org = "com.sksamuel.centurion"

   object Kotlin {
      const val reflect = "org.jetbrains.kotlin:kotlin-reflect:$kotlinVersion"
      const val stdlib = "org.jetbrains.kotlin:kotlin-stdlib:$kotlinVersion"
      const val datetime = "org.jetbrains.kotlinx:kotlinx-datetime:0.2.0"
      const val coroutines = "org.jetbrains.kotlinx:kotlinx-coroutines-core:1.6.0-RC"
   }

}

object Projects {
   const val schemas = ":centurion-schemas"
}
