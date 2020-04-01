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
    runtimeOnly("org.slf4j:slf4j-simple:${Library.SLF4J}")

    implementation("org.apache.curator:curator-test:${Library.CURATOR}")
    implementation("org.apache.hadoop:hadoop-minicluster:3.2.1")
}

