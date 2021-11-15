package io.qalipsis.plugins.cassandra.configuration

import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty



/**
 * Connection for operations using Datastax driver.
 * @property servers host:port, default [DefaultValues.server].
 * @property keyspace the top-level database object that controls the replication for the object it contains at each
 * datacenter in the cluster.
 * @property datacenterProfile an execution profile to encapsulate a group of settings that can then be associated with
 * individual queries.
 * @property datacenterName datacenter name.
 */
data class CassandraServerConfiguration (
    @field:NotEmpty var servers: List<String> = listOf(DefaultValues.server),
    @field:NotBlank var keyspace: String = "",
    var datacenterProfile: DriverProfile = DriverProfile.LOCAL,
    @field:NotBlank var datacenterName: String = "",
)


enum class DriverProfile {
    LOCAL, PEER
}

internal object DefaultValues {
    const val pollDurationInSeconds = 10L
    const val server = "localhost:27017"
}
