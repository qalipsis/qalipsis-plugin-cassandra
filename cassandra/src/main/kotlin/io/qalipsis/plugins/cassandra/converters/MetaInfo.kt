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