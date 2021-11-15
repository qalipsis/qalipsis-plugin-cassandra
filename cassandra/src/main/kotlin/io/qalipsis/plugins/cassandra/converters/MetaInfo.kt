package io.qalipsis.plugins.cassandra.converters

import com.datastax.oss.driver.api.core.CqlIdentifier
import com.datastax.oss.driver.api.core.cql.Row
import com.datastax.oss.driver.api.core.type.reflect.GenericType
import io.qalipsis.plugins.cassandra.poll.getJavaTypeFromCqlType

/**
 * Metadata extracted from Cassandra Row in a more convenient way for [DefaultCassandraConverter]
 *
 * @author Maxim Golokhov
 */
internal data class MetaInfo(
    val keyspace:String,
    val columnTypes: Map<CqlIdentifier, GenericType<*>>
) {
    constructor(row: Row): this(
        keyspace= row.columnDefinitions.first().keyspace.toString(),
        columnTypes = row.columnDefinitions.map {
            it.name to getJavaTypeFromCqlType(it.type)
        }.toMap()
    )
}