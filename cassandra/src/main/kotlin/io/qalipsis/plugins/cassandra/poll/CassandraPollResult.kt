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

package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.plugins.cassandra.CassandraQueryMeters
import io.qalipsis.plugins.cassandra.CassandraRecord

/**
 * Wrapper for the result of poll in Cassandra.
 *
 * @author Svetlana Paliashchuk
 *
 * @property records list of Cassandra records.
 * @property meters meters of the poll step.
 *
 */
class CassandraPollResult(
    val records: List<CassandraRecord<Map<CqlIdentifier, Any?>>>,
    val meters: CassandraQueryMeters
) : Iterable<CassandraRecord<Map<CqlIdentifier, Any?>>> {
    override fun iterator(): Iterator<CassandraRecord<Map<CqlIdentifier, Any?>>> {
        return records.iterator()
    }
}