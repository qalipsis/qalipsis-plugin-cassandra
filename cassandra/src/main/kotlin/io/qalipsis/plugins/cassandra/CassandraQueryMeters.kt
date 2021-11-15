package io.qalipsis.plugins.cassandra

import java.time.Duration

/**
 * Wrapper for the meters of the Cassandra search operations.
 *
 * @author Gabriel Moraes
 */
data class CassandraQueryMeters(
    var recordsCount: Int = 0,
    var timeToResponse: Duration? = null
)