description = "Module for running HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    implementation(project(":htable-protocol"))
    api("org.apache.curator:curator-framework:${Library.CURATOR}")

    implementation("com.typesafe.scala-logging:scala-logging_${Library.SCALA_LIB}:${Library.SCALA_LOGGING}")
    implementation("org.apache.curator:curator-recipes:${Library.CURATOR}")
}

