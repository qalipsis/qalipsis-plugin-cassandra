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

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.search.CassandraSearchResult
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [CassandraResultSetConverter], that converts a list of native cassandra rows to a list of items
 * in the output channel
 *
 * @author Gabriel Moraes
 */
internal class CassandraResultSetBatchRecordConverter<I>() :
    CassandraResultSetConverter<CassandraQueryResult, CassandraSearchResult<I>, I>,
    DefaultCassandraConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: CassandraQueryResult,
        input: I,
        output: StepOutput<CassandraSearchResult<I>>
    ) {
        tryAndLogOrNull(log) {
            output.send(CassandraSearchResult(
                input,
                records = convert(offset, value.rows),
                meters = value.meters
            ))
        }
    }


    companion object {

        @JvmStatic
        private val log = logger()
    }

}
