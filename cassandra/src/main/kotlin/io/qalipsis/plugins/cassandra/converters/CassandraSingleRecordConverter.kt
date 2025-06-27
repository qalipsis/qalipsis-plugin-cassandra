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
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.CassandraRecord
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of native Cassandra rows and forwards each of
 * them converted to a [CassandraRecord].
 *
 * @author Maxim Golokhov
 */
internal class CassandraSingleRecordConverter(
) : DatasourceObjectConverter<CassandraQueryResult, CassandraRecord<Map<CqlIdentifier, *>>>,
    DefaultCassandraConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: CassandraQueryResult,
        output: StepOutput<CassandraRecord<Map<CqlIdentifier, *>>>
    ) {

        tryAndLogOrNull(log) {
            val results = convert(offset, value.rows)
            results.forEach {
                output.send(
                    it
                )
            }
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }
}
