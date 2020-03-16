import com.google.protobuf.gradle.*

plugins {
    `java-library`
    id("com.google.protobuf")
    idea
}

repositories {
    mavenCentral()
}

protobuf {
    protoc {
        artifact = "com.google.protobuf:protoc:3.11.4"
    }

    plugins {
        id("grpc") {
            artifact = "io.grpc:protoc-gen-grpc-java:1.28.0"
        }
    }
    generateProtoTasks {
        ofSourceSet("main").forEach {
            it.plugins {
                id("grpc")
            }
        }
    }
}

idea {
    module {
        generatedSourceDirs.add(file("${protobuf.protobuf.generatedFilesBaseDir}/main/java"))
        generatedSourceDirs.add(file("${protobuf.protobuf.generatedFilesBaseDir}/main/grpc"))
    }
}

dependencies {
    implementation("io.grpc:grpc-netty-shaded:1.28.0")
    implementation("io.grpc:grpc-protobuf:1.28.0")
    implementation("io.grpc:grpc-stub:1.28.0")

    compileOnly("javax.annotation:javax.annotation-api:1.2")
}
