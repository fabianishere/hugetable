plugins {
    `java-library`
    scala
    id("cz.alenkacz.gradle.scalafmt")
}

/* Project configuration */
repositories {
    mavenCentral()
    jcenter()
}

tasks.test {
    useJUnitPlatform {
        includeEngines("scalatest")
        testLogging {
            events("passed", "skipped", "failed")
        }
    }
    reports.html.isEnabled = true
}


dependencies {
    implementation("org.scala-lang:scala-library:${Library.SCALA}")

    testImplementation("org.scalatest:scalatest_${Library.SCALA_LIB}:${Library.SCALATEST}")
    testRuntimeOnly("org.junit.jupiter:junit-jupiter-engine:${Library.JUNIT_JUPITER}")
    testRuntimeOnly("co.helmethair:scalatest-junit-runner:${Library.JUNIT_SCALATEST}")
}

