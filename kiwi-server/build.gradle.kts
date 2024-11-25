plugins {
    application
}

application {
    mainClass.set("kiwi.server.resp.Server")
}

dependencies {
    implementation(project(":kiwi-core"))

    implementation(libs.netty)
    implementation(libs.logback)
}