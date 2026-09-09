plugins {
    kotlin("jvm") version "2.4.0"
}

group = "org.example"
version = "1.0-SNAPSHOT"

repositories {
    mavenCentral()
}

dependencies {
    testImplementation(kotlin("test"))

    implementation("gov.nasa.jpl.parakeet:parakeet")

    // Not sure which of these are strictly needed.
    // TODO: Once a working implementation is in place, try removing each of these to cut down dependencies.
    implementation("org.apache.parquet:parquet-common:1.17.0")
    implementation("org.apache.parquet:parquet-encoding:1.17.0")
    implementation("org.apache.parquet:parquet-column:1.17.0")
    implementation("org.apache.parquet:parquet-hadoop:1.17.0")
    implementation("org.apache.hadoop:hadoop-client-api:3.4.0")

    implementation("org.apache.parquet:parquet-avro:1.17.0")
}

kotlin {
    jvmToolchain(21)
}

tasks.test {
    useJUnitPlatform()
}