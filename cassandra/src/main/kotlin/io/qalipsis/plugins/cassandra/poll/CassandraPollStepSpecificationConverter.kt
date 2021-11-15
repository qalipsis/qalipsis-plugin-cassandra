package io.qalipsis.plugins.cassandra.poll

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.api.steps.datasource.DatasourceObjectConverter
import io.qalipsis.api.steps.datasource.IterativeDatasourceStep
import io.qalipsis.api.steps.datasource.processors.NoopDatasourceObjectProcessor
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.configuration.CqlSessionBuilderFactory
import io.qalipsis.plugins.cassandra.converters.CassandraBatchRecordConverter
import io.qalipsis.plugins.cassandra.converters.CassandraSingleRecordConverter
import io.qalipsis.plugins.cassandra.search.CassandraQueryClientImpl
import jakarta.inject.Named
import kotlinx.coroutines.CoroutineScope
import kotlin.coroutines.CoroutineContext

/**
 * [StepSpecificationConverter] from [CassandraPollStepSpecificationImpl] to [CassandraIterativeReader] for a data source.
 *
 * @author Maxim Golokhov
 */
@StepConverter
internal class CassandraPollStepSpecificationConverter(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineScope: CoroutineScope,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineContext: CoroutineContext
) : StepSpecificationConverter<CassandraPollStepSpecificationImpl> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is CassandraPollStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<CassandraPollStepSpecificationImpl>) {
        val spec = creationContext.stepSpecification
        val cqlPollStatement = buildCqlStatement(spec)
        val stepId = spec.name
        val sessionBuilder = CqlSessionBuilderFactory(spec.serversConfig).buildSessionBuilder()
        val reader = CassandraIterativeReader(
            ioCoroutineScope = ioCoroutineScope,
            sessionBuilder = sessionBuilder,
            cqlPollStatement = cqlPollStatement,
            pollPeriod = spec.pollPeriod,
            cassandraQueryClient = CassandraQueryClientImpl(
                ioCoroutineContext,
                supplyIf(spec.monitoringConfig.events) { eventsLogger },
                supplyIf(spec.monitoringConfig.meters) { meterRegistry },
                "poll"
            ),
        )

        val converter = buildConverter(stepId, spec)

        val step = IterativeDatasourceStep(
            stepId,
            reader,
            NoopDatasourceObjectProcessor(),
            converter
        )
        creationContext.createdStep(step)
    }

    fun buildCqlStatement(
        spec: CassandraPollStepSpecificationImpl
    ): CqlPollStatement {
        return CqlPollStatementImpl(
            query = spec.query,
            parameters = spec.parameters,
            tieBreaker = spec.tieBreakerConfig,
        )
    }

    fun buildConverter(
        stepId: String,
        spec: CassandraPollStepSpecificationImpl,
    ): DatasourceObjectConverter<CassandraQueryResult, out Any> {

        return if (spec.flattenOutput) {
            CassandraSingleRecordConverter()
        } else {
            CassandraBatchRecordConverter()
        }
    }
}
