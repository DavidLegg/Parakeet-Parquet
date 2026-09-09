plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.serialization") version "2.4.10"
    kotlin("plugin.dataframe") version "2.4.10"
}

group = "gov.nasa.jpl.parakeet"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("gov.nasa.jpl.parakeet:parakeet")

    implementation("org.apache.parquet:parquet-common:1.17.0")
    implementation("org.apache.parquet:parquet-encoding:1.17.0")
    implementation("org.apache.parquet:parquet-column:1.17.0")
    implementation("org.apache.parquet:parquet-hadoop:1.17.0")
    implementation("org.apache.hadoop:hadoop-client-api:3.4.0")
    implementation("org.apache.hadoop:hadoop-client-runtime:3.4.0")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-datetime:0.8.0")
    implementation("org.jetbrains.kotlinx:kotlinx-coroutines-core:1.11.0")
    implementation("org.jetbrains.kotlin:kotlin-reflect:2.4.10")

    // Kotlin's DataFrame library is used to read Parquet files in tests, verifying what we wrote is correct.
    testImplementation("org.jetbrains.kotlinx:dataframe:1.0.0-rc01")

    // Provides an SLF4J binding so tests don't print "No SLF4J providers were found" warnings.
    testImplementation("org.slf4j:slf4j-nop:2.0.16")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
    jvmArgs(
        // Required to read parquet files
        "--add-opens=java.base/java.nio=ALL-UNNAMED"
    )
}