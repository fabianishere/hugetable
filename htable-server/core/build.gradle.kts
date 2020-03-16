description = "Module for running HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
    `grpc-convention`
}

dependencies {
    api(project(":htable-api"))
    api("org.apache.curator:curator-framework:${Library.CURATOR}")

    implementation("com.typesafe.scala-logging:scala-logging_${Library.SCALA_LIB}:${Library.SCALA_LOGGING}")
    implementation("org.apache.curator:curator-recipes:${Library.CURATOR}")
}

