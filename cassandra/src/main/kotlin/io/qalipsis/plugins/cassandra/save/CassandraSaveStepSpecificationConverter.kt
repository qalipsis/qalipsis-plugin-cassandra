package io.qalipsis.plugins.cassandra.save

import io.micrometer.core.instrument.MeterRegistry
import io.qalipsis.api.Executors
import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.cassandra.configuration.CqlSessionBuilderFactory
import jakarta.inject.Named
import kotlin.coroutines.CoroutineContext


/**
 * [StepSpecificationConverter] from [CassandraSaveStepSpecificationImpl] to [CassandraSaveStep]
 * to use the Save API.
 *
 * @author Svetlana Paliashchuk
 */
@StepConverter
internal class CassandraSaveStepSpecificationConverter(
    private val meterRegistry: MeterRegistry,
    private val eventsLogger: EventsLogger,
    @Named(Executors.IO_EXECUTOR_NAME) private val ioCoroutineContext: CoroutineContext
) : StepSpecificationConverter<CassandraSaveStepSpecificationImpl<*>> {

    override fun support(stepSpecification: StepSpecification<*, *, *>): Boolean {
        return stepSpecification is CassandraSaveStepSpecificationImpl
    }

    override suspend fun <I, O> convert(creationContext: StepCreationContext<CassandraSaveStepSpecificationImpl<*>>) {
        val spec = creationContext.stepSpecification
        val stepId = spec.name
        val sessionBuilder = CqlSessionBuilderFactory(spec.serversConfig).buildSessionBuilder()

        @Suppress("UNCHECKED_CAST")
        val step = CassandraSaveStep(
            id = stepId,
            retryPolicy = spec.retryPolicy,
            sessionBuilder = sessionBuilder,
            tableName = spec.tableName,
            columns = spec.columnsConfig,
            rowsFactory = spec.rowsFactory,
            cassandraSaveQueryClient = CassandraSaveQueryClientImpl(
                ioCoroutineContext,
                supplyIf(spec.monitoringConfig.events) { eventsLogger },
                supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            )
        )
        creationContext.createdStep(step)
    }

}
