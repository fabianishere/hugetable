description = "Core data model for HTable client and servers"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    api("com.typesafe.akka:akka-actor-typed_${Library.SCALA_LIB}:${Library.AKKA}")
}
