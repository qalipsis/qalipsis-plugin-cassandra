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

import com.datastax.oss.driver.api.core.CqlSession
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.plugins.cassandra.CassandraQueryResult

/**
 * Client to query from Cassandra.
 *
 * @author Gabriel Moraes
 */
interface CassandraQueryClient {

    suspend fun start(context: StepStartStopContext)

    suspend fun stop(context: StepStartStopContext)

    /**
     * Executes a query and returns the wrapper for metrics and list of results.
     */
    suspend fun execute(
        session: CqlSession,
        query: String,
        parameters: List<Any>,
        contextEventTags: Map<String, String>
    ): CassandraQueryResult
}


