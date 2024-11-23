plugins {
    java
    idea
}

allprojects {
    group = "kiwi"
    version = "0.1.0-SNAPSHOT"

    repositories {
        mavenCentral()
    }
}

subprojects {
    apply(plugin = "java")

    dependencies {
        implementation(rootProject.libs.slf4j.api)
        implementation(rootProject.libs.typesafe.config)

        testImplementation(platform(rootProject.libs.junit.bom))
        testImplementation(rootProject.libs.junit.jupiter)
        testRuntimeOnly(rootProject.libs.junit.platform)
    }

    java {
        sourceCompatibility = JavaVersion.VERSION_21
    }

    tasks.test {
        useJUnitPlatform()
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
}
