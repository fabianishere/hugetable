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
    api(project(":htable-server:core"))

    implementation("org.rogach:scallop_2.13:3.4.0")
    runtimeOnly("org.slf4j:slf4j-simple:${Library.SLF4J}")
}

