package io.qalipsis.plugins.cassandra.converters

import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.cassandra.search.CassandraSearchResult
import io.qalipsis.plugins.cassandra.CassandraQueryResult
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
