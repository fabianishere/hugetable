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

    implementation("com.typesafe.scala-logging:scala-logging_${Library.SCALA_LIB}:${Library.SCALA_LOGGING}")
    implementation("org.apache.curator:curator-framework:${Library.CURATOR}")
    implementation("org.apache.curator:curator-recipes:${Library.CURATOR}")

    runtimeOnly("org.slf4j:slf4j-simple:${Library.SLF4J}")
}

