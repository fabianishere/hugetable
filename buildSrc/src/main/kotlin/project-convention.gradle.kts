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
    useJUnitPlatform()
    reports.html.isEnabled = true
}


dependencies {
    implementation("org.scala-lang:scala-library:2.13.1")
}

