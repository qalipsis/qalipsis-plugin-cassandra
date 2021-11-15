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

            withLocalDatacenter(serversConfig.datacenterProfile.toString(), serversConfig.datacenterName)
        }
    }

    private val String.toInetAddress: InetSocketAddress
        get() {
            val uri = URI("http://$this")
            return InetSocketAddress(uri.host, uri.port)
        }
}
