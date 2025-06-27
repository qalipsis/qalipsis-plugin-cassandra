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

package io.qalipsis.plugins.cassandra.save

import io.qalipsis.api.annotations.StepConverter
import io.qalipsis.api.events.EventsLogger
import io.qalipsis.api.lang.supplyIf
import io.qalipsis.api.meters.CampaignMeterRegistry
import io.qalipsis.api.steps.StepCreationContext
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.StepSpecificationConverter
import io.qalipsis.plugins.cassandra.configuration.CqlSessionBuilderFactory


/**
 * [StepSpecificationConverter] from [CassandraSaveStepSpecificationImpl] to [CassandraSaveStep]
 * to use the Save API.
 *
 * @author Svetlana Paliashchuk
 */
@StepConverter
internal class CassandraSaveStepSpecificationConverter(
    private val meterRegistry: CampaignMeterRegistry,
    private val eventsLogger: EventsLogger,
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
                supplyIf(spec.monitoringConfig.events) { eventsLogger },
                supplyIf(spec.monitoringConfig.meters) { meterRegistry }
            )
        )
        creationContext.createdStep(step)
    }

}
