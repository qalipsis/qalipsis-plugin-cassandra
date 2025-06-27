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

package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraRecord

/**
 * Wrapper for the result of search query procedure in Cassandra.
 *
 * @author Svetlana Paliashchuk
 *
 * @property input input for the search step.
 * @property records list of Cassandra records retrieved from DB.
 * @property meters meters of the search step.
 *
 */
class CassandraSearchResult<I>(
    val input: I,
    val records: List<CassandraRecord<Map<CqlIdentifier, Any?>>>,
    val meters: CassandraQueryMeters
) : Iterable<CassandraRecord<Map<CqlIdentifier, Any?>>> {

    override fun iterator(): Iterator<CassandraRecord<Map<CqlIdentifier, Any?>>> {
        return records.iterator()
    }
}