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

package io.qalipsis.plugins.cassandra.save

import com.datastax.oss.driver.api.core.CqlSession
import io.qalipsis.api.context.StepStartStopContext

/**
 * Client to save rows in Cassandra.
 *
 * @author Svetlana Paliashchuk
 */
internal interface CassandraSaveQueryClient {

    suspend fun start(context: StepStartStopContext)

    suspend fun stop(context: StepStartStopContext)

    /**
     * Executes a query.
     */
    suspend fun execute(
        session: CqlSession,
        tableName: String,
        columns: List<String>,
        rows: List<CassandraSaveRow>,
        contextEventTags: Map<String, String>
    ): CassandraSaveQueryMeters
}


