description = "Module for running HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    api("com.typesafe.akka:akka-actor-typed_${Library.SCALA_LIB}:${Library.AKKA}")
    api("org.apache.curator:curator-framework:${Library.CURATOR}")

    implementation(project(":htable-protocol"))
    implementation("org.apache.curator:curator-recipes:${Library.CURATOR}")
}

