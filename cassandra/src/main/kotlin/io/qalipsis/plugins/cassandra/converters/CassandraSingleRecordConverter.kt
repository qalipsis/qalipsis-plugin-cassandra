/*
 * Copyright 2022 AERIS IT Solutions GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing
 * permissions and limitations under the License.
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
