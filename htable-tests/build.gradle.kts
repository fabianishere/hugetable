description = "Test suite for HugeTable."

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    testImplementation(project(":htable-server"))
    testImplementation(project(":htable-client"))
    testImplementation("org.apache.curator:curator-test:${Library.CURATOR}")
    testImplementation("org.apache.hadoop:hadoop-minicluster:3.1.3")
}

