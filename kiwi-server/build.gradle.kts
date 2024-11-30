plugins {
    application
}

application {
    mainClass.set("kiwi.server.resp.Server")

    applicationDefaultJvmArgs = listOf(
        "-Xms2g",
        "-Xmx2g",
        "-XX:+AlwaysPreTouch",
        "-XX:+UseG1GC",
        "-XX:+UseStringDeduplication",
    )
}

dependencies {
    implementation(project(":kiwi-core"))

    implementation(libs.netty)
    implementation(libs.logback)
}