description = "Storage driver for HugeTable using HBase"

/* Build configuration */
plugins {
    `project-convention`
}

dependencies {
    api(project(":htable-storage"))
    implementation("com.typesafe.scala-logging:scala-logging_${Library.SCALA_LIB}:${Library.SCALA_LOGGING}")


    implementation("org.apache.hbase:hbase:2.2.4")
    implementation("org.apache.hbase:hbase-common:2.2.4")
    implementation("org.apache.hbase:hbase-server:2.2.4")
    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    implementation("org.apache.hadoop:hadoop-client:3.2.1")
    implementation("org.apache.hadoop:hadoop-hdfs:3.2.1")

    testImplementation("org.apache.hadoop:hadoop-minicluster:3.2.1")
}
