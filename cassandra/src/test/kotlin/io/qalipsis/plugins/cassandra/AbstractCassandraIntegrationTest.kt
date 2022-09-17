/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package io.qalipsis.plugins.cassandra

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.cassandra.poll.Item
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.io.readResourceLines
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.extension.RegisterExtension
import org.testcontainers.containers.CassandraContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.junit.jupiter.Container
import org.testcontainers.junit.jupiter.Testcontainers
import org.testcontainers.utility.DockerImageName
import java.net.InetSocketAddress
import kotlin.math.pow

@Testcontainers
internal abstract class AbstractCassandraIntegrationTest {

    lateinit var sessionBuilder: CqlSessionBuilder

    lateinit var session: CqlSession

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    @BeforeAll
    fun setUpSessionBuilder() {
        val port = CONTAINER.getMappedPort(CassandraContainer.CQL_PORT)
        sessionBuilder = CqlSession.builder()
            .addContactPoint(InetSocketAddress("localhost", port))
            .withLocalDatacenter(DATACENTER)
            .withKeyspace(KEYSPACE)
    }

    fun executeScript(name: String) {
        log.debug { "Executing resource script $name" }
        readResourceLines(name).forEach { session.execute(it) }
    }

    fun readFromCsv(name: String): List<Item> {
        log.debug { "Executing CSV data from resource $name" }
        return readResourceLines(name).map { Item(it.split(";")) }
    }

    fun tearDown() {
        session.close()
    }

    companion object {
        @JvmStatic
        private val log = logger()

        private const val CASSANDRA_DOCKER_IMAGE = "cassandra:3"

        @JvmStatic
        private val IMAGE_NAME: DockerImageName = DockerImageName.parse(CASSANDRA_DOCKER_IMAGE)

        const val DATACENTER = "datacenter1"

        const val KEYSPACE = "keySpaceTest"

        @JvmStatic
        @Container
        val CONTAINER = CassandraContainer<Nothing>(IMAGE_NAME)
            .apply {
                withCreateContainerCmdModifier { cmd ->
                    cmd.hostConfig!!.withMemory(512 * 1024.0.pow(2).toLong()).withCpuCount(2)
                }
                withEnv("MAX_HEAP_SIZE", "256M")
                withEnv("HEAP_NEWSIZE", "128M")
                waitingFor(Wait.forListeningPort())
                withInitScript("input/create_keyspace_and_table.cql")
            }
    }

}
