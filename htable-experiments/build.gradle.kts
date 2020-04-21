description = "Experiments for HugeTable implementation"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    implementation(project(":htable-client"))
    implementation("org.rogach:scallop_${Library.SCALA_LIB}:${Library.SCALLOP}")
    runtimeOnly("org.slf4j:slf4j-simple:${Library.SLF4J}")
}
