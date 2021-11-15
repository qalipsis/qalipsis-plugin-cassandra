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
): DatasourceObjectConverter<CassandraQueryResult, CassandraRecord<Map<CqlIdentifier, *>>>, DefaultCassandraConverter() {

    override suspend fun supply(offset: AtomicLong, value: CassandraQueryResult, output: StepOutput<CassandraRecord<Map<CqlIdentifier, *>>>) {

        tryAndLogOrNull(log){
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
