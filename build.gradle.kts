plugins {
    application
    idea
}

group = "kiwi"
version = "0.1.0-SNAPSHOT"

application {
    mainClass = "kiwi.Kiwi"
}

java {
    sourceCompatibility = JavaVersion.VERSION_21
}

repositories {
    mavenCentral()
}

dependencies {
    implementation(libs.slf4j)
    implementation(libs.logback)
    implementation(libs.typesafe.config)
    implementation(libs.zero.allocation.hashing)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
    }
}
