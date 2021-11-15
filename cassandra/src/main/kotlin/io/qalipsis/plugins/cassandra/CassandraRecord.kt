package io.qalipsis.plugins.cassandra

/**
 * @author Maxim Golokhov
 */
data class CassandraRecord<V>(
        val id: Long = 0,
        val source: String,
        val offset: Long,
        /**
         * Timestamp when the message was fetched.
         */
        val receivedTimestamp: Long = System.currentTimeMillis(),
        val value: V
)