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
