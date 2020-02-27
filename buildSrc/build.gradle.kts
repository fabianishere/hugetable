plugins {
    `kotlin-dsl`
}

kotlinDslPluginOptions {
    experimentalWarning.set(false)
}


/* Project configuration */
repositories {
    jcenter()
    maven {
        url = uri("https://plugins.gradle.org/m2/")
    }
}

dependencies {
    implementation("gradle.plugin.cz.alenkacz:gradle-scalafmt:1.13.0")
}
