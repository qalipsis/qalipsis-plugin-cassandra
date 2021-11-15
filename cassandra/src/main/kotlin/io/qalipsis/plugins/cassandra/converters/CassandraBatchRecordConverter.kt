package io.qalipsis.plugins.cassandra.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.poll.CassandraPollResult
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [DatasourceObjectConverter], that reads a batch of native Cassandra rows and forwards it
 * as a [CassandraPollResult].
 *
 * @author Maxim Golokhov
 */
internal class CassandraBatchRecordConverter(
) : DatasourceObjectConverter<CassandraQueryResult, CassandraPollResult>, DefaultCassandraConverter() {

    override suspend fun supply(
        offset: AtomicLong,
        value: CassandraQueryResult,
        output: StepOutput<CassandraPollResult>
    ) {

        tryAndLogOrNull(log) {
            output.send(CassandraPollResult(
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
