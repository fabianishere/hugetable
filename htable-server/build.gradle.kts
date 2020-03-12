description = "Module for running HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
    application
}

application {
    mainClassName = "nl.tudelft.htable.server.Main"
}

dependencies {
    api(project(":htable-api"))

    implementation("com.typesafe.scala-logging:scala-logging_2.13:3.9.2")
    implementation("org.apache.curator:curator-framework:4.3.0")
    implementation("org.apache.curator:curator-recipes:4.3.0")

    runtimeOnly("org.slf4j:slf4j-simple:1.7.30")
}

