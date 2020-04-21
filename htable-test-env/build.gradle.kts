description = "Utility for booting embedded test environment for HugeTable without external dependencies."

/* Build configuration */
plugins {
    `project-convention`
    application
}

application {
    mainClassName = "nl.tudelft.htable.test.env.Main"
}

dependencies {
    implementation("com.typesafe.scala-logging:scala-logging_${Library.SCALA_LIB}:${Library.SCALA_LOGGING}")
    implementation("org.rogach:scallop_${Library.SCALA_LIB}:${Library.SCALLOP}")

    implementation("org.apache.curator:curator-test:${Library.CURATOR}")
    implementation("org.apache.hadoop:hadoop-minicluster:3.1.3")
}

