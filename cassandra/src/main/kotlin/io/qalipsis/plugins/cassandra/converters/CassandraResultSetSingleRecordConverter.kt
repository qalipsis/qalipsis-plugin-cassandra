package io.qalipsis.plugins.cassandra.converters

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.api.context.StepOutput
import io.qalipsis.api.lang.tryAndLogOrNull
import io.qalipsis.api.logging.LoggerHelper.logger
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import java.util.concurrent.atomic.AtomicLong

/**
 * Implementation of [CassandraResultSetConverter], that converts a list of native cassandra rows to single items
 * in the output channel
 *
 * @author Gabriel Moraes
 */
internal class CassandraResultSetSingleRecordConverter<I>: CassandraResultSetConverter<CassandraQueryResult, Pair<I, CassandraRecord<Map<CqlIdentifier, *>>>, I>, DefaultCassandraConverter() {

    override suspend fun supply(offset: AtomicLong, value: CassandraQueryResult, input: I, output: StepOutput<Pair<I, CassandraRecord<Map<CqlIdentifier, *>>>>) {

        tryAndLogOrNull(log){
                convert(offset, value.rows).forEach {
                output.send(input to it)
            }
        }
    }

    companion object {

        @JvmStatic
        private val log = logger()
    }

}
