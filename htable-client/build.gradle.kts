description = "Client library for communicating with HugeTable servers"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    api(project(":htable-api"))
}
