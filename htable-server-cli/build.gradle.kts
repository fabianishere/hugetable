description = "Command line interface for HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
    application
}

application {
    mainClassName = "nl.tudelft.htable.server.cli.Main"
}

dependencies {
    implementation(project(":htable-server"))
    implementation("org.rogach:scallop_${Library.SCALA_LIB}:${Library.SCALLOP}")
    runtimeOnly("org.slf4j:slf4j-simple:${Library.SLF4J}")
}

