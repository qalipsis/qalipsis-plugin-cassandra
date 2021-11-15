package io.qalipsis.plugins.cassandra.search

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.cassandra.CassandraQueryResult
import io.qalipsis.plugins.cassandra.configuration.CqlSessionBuilderFactory
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetBatchRecordConverter
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetConverter
import io.qalipsis.plugins.cassandra.converters.CassandraResultSetSingleRecordConverter
import jakarta.inject.Named
import kotlin.coroutines.CoroutineContext

/**
 * [StepSpecificationConverter] from [CassandraSearchStepSpecificationImpl] to [CassandraSearchStep]
 * to use the Search API.
 *
 * @author Gabriel Moraes
 */
@StepConverter
internal class CassandraSearchStepSpecificationConverter(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineContext: CoroutineContext
) : StepSpecificationConverter<CassandraSearchStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is CassandraSearchStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<CassandraSearchStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name
        val sessionBuilder = CqlSessionBuilderFactory(spec.serversConfig).buildSessionBuilder()

        @Suppress("UNCHECKED_CAST")
        val step = CassandraSearchStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            sessionBuilder = sessionBuilder,
            queryFactory = spec.queryFactory as suspend (ctx: StepContext<*, *>, input: Any?) -> String,
            parametersFactory = spec.parametersFactory as suspend (ctx: StepContext<*, *>, input: Any?) -> List<Any>,
            converter = buildConverter(spec) as CassandraResultSetConverter<CassandraQueryResult, Any, Any?>,
            cassandraQueryClient = CassandraQueryClientImpl(
                ioCoroutineContext,
                supplyIf(spec.monitoringConfig.events) { eventsLogger },
                supplyIf(spec.monitoringConfig.meters) { meterRegistry },
                "search"
            )
        )
        creationContext.createdStep(step)
    }


    private fun buildConverter(
        spec: CassandraSearchStepSpecificationImpl<*>
    ): CassandraResultSetConverter<CassandraQueryResult, out Any, *> {

        return if (spec.flattenOutput) {
            CassandraResultSetSingleRecordConverter<Any>()
        } else {
            CassandraResultSetBatchRecordConverter<Any>()
        }
    }

}
