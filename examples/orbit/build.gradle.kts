plugins {
    kotlin("jvm") version "2.4.10"
    kotlin("plugin.serialization") version "2.4.10"
    application
}

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("gov.nasa.jpl.parakeet:parakeet")
    implementation(project(":"))

    implementation("org.apache.parquet:parquet-common:1.17.0")
    implementation("org.apache.parquet:parquet-encoding:1.17.0")
    implementation("org.apache.parquet:parquet-column:1.17.0")
    implementation("org.apache.parquet:parquet-hadoop:1.17.0")
    implementation("org.apache.hadoop:hadoop-client-api:3.4.0")
    implementation("org.apache.hadoop:hadoop-client-runtime:3.4.0")

    implementation("org.jetbrains.kotlinx:kotlinx-serialization-core:1.11.0")
    implementation("org.jetbrains.kotlinx:kotlinx-serialization-json:1.11.0")

    // Provides an SLF4J binding to suppress "No SLF4J providers were found" warnings.
    implementation("org.slf4j:slf4j-nop:2.0.16")
}

application {
    mainClass.set("examples.orbit.MainKt")
}

kotlin {
    jvmToolchain(21)
}
