package io.qalipsis.plugins.cassandra.search

import com.datastax.oss.driver.api.core.CqlIdentifier
import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.ConfigurableStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.cassandra.CassandraRecord
import io.qalipsis.plugins.cassandra.CassandraStepSpecification
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration

/**
 * Specification for a [io.qalipsis.plugins.cassandra.search.CassandraSearchStep] to search data from a Cassandra.
 *
 * The output is a pair of [I] and a list of [CassandraRecord]  contains maps of column [CqlIdentifier] to values.
 *
 * When [flatten] it is a map of column [CqlIdentifier] to values.
 *
 * @author Gabriel Moraes
 */
interface CassandraSearchStepSpecification<I> :
    StepSpecification<I, CassandraSearchResult<I>, CassandraSearchStepSpecification<I>>,
    ConfigurableStepSpecification<I, CassandraSearchResult<I>, CassandraSearchStepSpecification<I>>,
    CassandraStepSpecification<I, CassandraSearchResult<I>, CassandraSearchStepSpecification<I>> {

    /**
     * Configures connection to the Cassandra cluster.
     */
    fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit)

    /**
     * Defines the prepared statement to execute when searching. The query may contain ordering clauses
     */
    fun query(queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> String)

    /**
     * Builder for the options to add as query parameters. Defaults to no parameter
     */
    fun parameters(parametersFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Any>)

    /**
     * Configures the monitoring of the search step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)
}

/**
 * Implementation of [CassandraSearchStepSpecification].
 *
 * @author Gabriel Moraes
 */
@Spec
internal class CassandraSearchStepSpecificationImpl<I> :
    CassandraSearchStepSpecification<I>,
    AbstractStepSpecification<I, CassandraSearchResult<I>, CassandraSearchStepSpecification<I>>() {

    internal var serversConfig = CassandraServerConfiguration()

    internal var queryFactory: (suspend (ctx: StepContext<*, *>, input: I) -> String) =
        { _, _ -> "" }

    internal var parametersFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<Any>) =
        { _, _ -> emptyList() }

    internal var monitoringConfig = StepMonitoringConfiguration()

    override fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit) {
        serversConfig.serverConfiguration()
    }

    override fun query(queryFactory: suspend (ctx: StepContext<*, *>, input: I) -> String) {
        this.queryFactory = queryFactory
    }

    override fun parameters(parametersFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<Any>) {
        this.parametersFactory = parametersFactory
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }
}


/**
 * Searches data in Cassandra using a search query.
 *
 * @author Gabriel Moraes
 */
fun <I> CassandraStepSpecification<*, I, *>.search(
    configurationBlock: CassandraSearchStepSpecification<I>.() -> Unit
): CassandraSearchStepSpecification<I> {
    val step = CassandraSearchStepSpecificationImpl<I>()
    step.configurationBlock()

    this.add(step)
    return step
}



