/*
 * QALIPSIS
 * Copyright (C) 2025 AERIS IT Solutions GmbH
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 */

plugins {
    kotlin("jvm")
    kotlin("kapt")
    kotlin("plugin.allopen")
    `java-test-fixtures`
}

description = "QALIPSIS plugin for Apache Cassandra"

kapt {
    correctErrorTypes = true
    useBuildCache = false
}

allOpen {
    annotations(
        "io.micronaut.aop.Around",
        "jakarta.inject.Singleton",
        "io.qalipsis.api.annotations.StepConverter",
        "io.qalipsis.api.annotations.StepDecorator",
        "io.qalipsis.api.annotations.PluginComponent",
        "io.qalipsis.api.annotations.Spec",
        "io.micronaut.validation.Validated"
    )
}

kotlin.sourceSets["test"].kotlin.srcDir("build/generated/source/kaptKotlin/catadioptre")
kapt.useBuildCache = false

val pluginPlatformVersion: String by project
val cassandraDriverVersion = "4.13.0"
val cassandraAllDriverVersion = "4.0.2"

dependencies {
    implementation(platform("io.qalipsis:qalipsis-plugin-platform:${pluginPlatformVersion}"))

    compileOnly("io.aeris-consulting:catadioptre-annotations")
    compileOnly("io.micronaut:micronaut-runtime")

    api("com.datastax.oss:java-driver-core:$cassandraDriverVersion")
    api("com.datastax.oss:java-driver-query-builder:$cassandraDriverVersion")
    api("com.datastax.oss:java-driver-mapper-runtime:$cassandraDriverVersion")
    api("org.apache.cassandra:cassandra-all:$cassandraAllDriverVersion")
    implementation(group = "io.netty", name = "netty-transport-native-epoll", classifier = "linux-x86_64")
    implementation(group = "io.netty", name = "netty-transport-native-kqueue", classifier = "osx-x86_64")

    api("io.qalipsis:qalipsis-api-common")
    api("io.qalipsis:qalipsis-api-dsl")

    kapt(platform("io.qalipsis:qalipsis-plugin-platform:${pluginPlatformVersion}"))
    kapt("io.qalipsis:qalipsis-api-processors")
    kapt("io.aeris-consulting:catadioptre-annotations")

    testFixturesImplementation(platform("io.qalipsis:qalipsis-plugin-platform:${pluginPlatformVersion}"))
    testFixturesImplementation("io.qalipsis:qalipsis-api-common")
    testFixturesImplementation("io.qalipsis:qalipsis-test")

    testImplementation(platform("io.qalipsis:qalipsis-plugin-platform:${pluginPlatformVersion}"))
    testImplementation("org.testcontainers:cassandra")
    testImplementation("io.qalipsis:qalipsis-test")
    testImplementation("io.qalipsis:qalipsis-api-dsl")
    testImplementation("io.qalipsis:qalipsis-runtime")
    testImplementation(testFixtures("io.qalipsis:qalipsis-api-dsl"))
    testImplementation(testFixtures("io.qalipsis:qalipsis-api-common"))
    testImplementation(testFixtures("io.qalipsis:qalipsis-runtime"))
    testImplementation("javax.annotation:javax.annotation-api")
    testImplementation("io.micronaut:micronaut-runtime")
    testImplementation("io.aeris-consulting:catadioptre-kotlin")
    testRuntimeOnly("io.qalipsis:qalipsis-head")
    testRuntimeOnly("io.qalipsis:qalipsis-factory")

    kaptTest(platform("io.qalipsis:qalipsis-plugin-platform:${pluginPlatformVersion}"))
    kaptTest("io.micronaut:micronaut-inject-java")
    kaptTest("io.qalipsis:qalipsis-api-processors")
}

