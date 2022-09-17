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
            converter = CassandraResultSetBatchRecordConverter<Any>() as CassandraResultSetConverter<CassandraQueryResult, Any, Any?>,
            cassandraQueryClient = CassandraQueryClientImpl(
                ioCoroutineContext,
                supplyIf(spec.monitoringConfig.events) { eventsLogger },
                supplyIf(spec.monitoringConfig.meters) { meterRegistry },
                "search"
            )
        )
        creationContext.createdStep(step)
    }

}
