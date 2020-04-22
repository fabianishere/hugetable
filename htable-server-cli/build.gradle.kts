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

    implementation("org.apache.hadoop:hadoop-client:3.0.3")
    implementation(project(":htable-storage-hbase"))
    implementation("org.apache.logging.log4j:log4j-slf4j18-impl:2.13.1")
    implementation("org.apache.logging.log4j:log4j-jcl:2.13.1")
}

