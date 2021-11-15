package io.qalipsis.plugins.cassandra

import com.datastax.oss.driver.api.core.cql.Row
import io.qalipsis.plugins.cassandra.save.CassandraSaveResult

/**
 * A wrapper for meters and rows used to initialize [CassandraSaveResult].
 *
 * @property meters metrics of the search step.
 * @property rows result of search query procedure in Cassandra.
 *
 * @author Svetlana Paliashchuk
 */
class CassandraQueryResult(
    val rows: List<Row>,
    val meters: CassandraQueryMeters
)