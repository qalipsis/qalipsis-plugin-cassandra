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

package io.qalipsis.plugins.cassandra.poll

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.scenario.StepSpecificationRegistry
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.BroadcastSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.LoopableSpecification
import io.qalipsis.api.steps.SingletonConfiguration
import io.qalipsis.api.steps.SingletonType
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.api.steps.UnicastSpecification
import io.qalipsis.plugins.cassandra.CassandraNamespaceScenarioSpecification
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.CassandraStepSpecification
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration
import io.qalipsis.plugins.cassandra.configuration.DefaultValues
import java.time.Duration
import javax.validation.constraints.NotBlank
import javax.validation.constraints.NotEmpty
import javax.validation.constraints.NotNull

/**
 * Specification for an [io.qalipsis.api.steps.datasource.IterativeDatasourceStep] to poll data from Apache Cassandra.
 *
 * The output is a list of [CassandraRecord] contains maps of column [CqlIdentifier] to values.
 *
 * When [flatten] it is a map of column [CqlIdentifier] to values.
 *
 * @author Maxim Golokhov
 */
interface CassandraPollStepSpecification :
    StepSpecification<Unit, CassandraPollResult, CassandraPollStepSpecification>,
    CassandraStepSpecification<Unit, CassandraPollResult, CassandraPollStepSpecification>,
    ConfigurableStepSpecification<Unit, CassandraPollResult, CassandraPollStepSpecification>,
    LoopableSpecification, UnicastSpecification, BroadcastSpecification {

    /**
     * Configures connection to the Cassandra cluster.
     */
    fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit)

    /**
     * Defines the prepared statement to execute when polling.
     */
    fun query(queryString: String)

    /**
     * Defines the parameters to be used in the query placeholders.
     */
    fun parameters(parameters: List<Any>)

    /**
     * Defines the parameters to be used in the query placeholders.
     */
    fun parameters(vararg parameters: Any) = parameters(parameters.toList())

    /**
     * Defines the tie-breaker column being set as first column to sort and its type.
     *
     */
    fun tieBreaker(tieBreakerConfiguration: TieBreaker.() -> Unit)

    /**
     * Duration between two executions of poll. Default value is 10 seconds.
     */
    fun pollDelay(duration: Duration)

    /**
     * Duration between two executions of poll. Default value is 10 seconds.
     */
    fun pollDelay(delayMillis: Long)

    /**
     * Configures the monitoring of the poll step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)

    /**
     * Returns the values individually.
     */
    fun flatten(): StepSpecification<Unit, CassandraRecord<Map<CqlIdentifier, Any?>>, *>
}

/**
 * Implementation of [CassandraPollStepSpecification].
 *
 * @author Maxim Golokhov
 */
@Spec
internal class CassandraPollStepSpecificationImpl :
    AbstractStepSpecification<Unit, CassandraPollResult, CassandraPollStepSpecification>(),
    CassandraPollStepSpecification {

    override val singletonConfiguration: SingletonConfiguration = SingletonConfiguration(SingletonType.UNICAST)

    internal var serversConfig = CassandraServerConfiguration()

    @field:NotBlank
    internal var query = ""

    @field:NotEmpty
    internal var parameters = emptyList<Any>()

    internal var tieBreakerConfig = TieBreaker()

    @field:NotNull
    internal var pollPeriod: Duration = Duration.ofSeconds(DefaultValues.pollDurationInSeconds)

    internal val monitoringConfig = StepMonitoringConfiguration()

    internal var flattenOutput = false

    override fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit) {
        serversConfig.serverConfiguration()
    }

    override fun query(queryString: String) {
        query = queryString
    }

    override fun parameters(parameters: List<Any>) {
        this.parameters = parameters
    }

    override fun tieBreaker(tieBreakerConfiguration: TieBreaker.() -> Unit) {
        tieBreakerConfig.tieBreakerConfiguration()
    }

    override fun pollDelay(delayMillis: Long) {
        pollPeriod = Duration.ofMillis(delayMillis)
    }

    override fun pollDelay(duration: Duration) {
        pollPeriod = duration
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

    override fun flatten(): StepSpecification<Unit, CassandraRecord<Map<CqlIdentifier, Any?>>, *> {
        flattenOutput = true

        @Suppress("UNCHECKED_CAST")
        return this as StepSpecification<Unit, CassandraRecord<Map<CqlIdentifier, Any?>>, *>
    }

}

/**
 * Creates a Poll step in order to periodically fetch data from a Cassandra database.
 *
 * This step is generally used in conjunction with a left join to assert data or inject them in a workflow.
 *
 * @author Maxim Golokhov
 */
fun CassandraNamespaceScenarioSpecification.poll(
    configurationBlock: CassandraPollStepSpecification.() -> Unit
): CassandraPollStepSpecification {
    val step = CassandraPollStepSpecificationImpl()
    step.configurationBlock()

    (this as StepSpecificationRegistry).add(step)
    return step
}



