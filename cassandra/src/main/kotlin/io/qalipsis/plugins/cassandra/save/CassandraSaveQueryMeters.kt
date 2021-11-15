package io.qalipsis.plugins.cassandra.save

import java.time.Duration

/**
 * Wrapper for the meters of the Cassandra save operation.
 *
 * @author Svetlana Paliashchuk
 */
data class CassandraSaveQueryMeters(
    val recordsToBeSent: Int,
    val timeToSuccess: Duration? = null,
    val savedDocuments: Int,
    val failedDocuments: Int
)