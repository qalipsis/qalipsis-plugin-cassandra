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

package io.qalipsis.plugins.cassandra.configuration

import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import java.net.InetSocketAddress
import java.net.URI

/**
 * Builder for cassandra session. See (here)[https://docs.datastax.com/en/drivers/java/4.0/com/datastax/oss/driver/api/core/session/SessionBuilder.html].
 *
 * @author Gabriel Moraes
 */
class CqlSessionBuilderFactory(
    private val serversConfig: CassandraServerConfiguration
) {

    fun buildSessionBuilder(): CqlSessionBuilder {
        return CqlSession.builder().apply {
            if (serversConfig.servers.isEmpty()) {
                addContactPoint(DefaultValues.server.toInetAddress)
            } else {
                serversConfig.servers.forEach {
                    addContactPoint(it.toInetAddress)
                }
            }

            withKeyspace(serversConfig.keyspace)
            withLocalDatacenter("${serversConfig.datacenterProfile}".lowercase(), serversConfig.datacenterName)
        }
    }

    private val String.toInetAddress: InetSocketAddress
        get() {
            val uri = URI("http://$this")
            return InetSocketAddress(uri.host, uri.port)
        }
}
