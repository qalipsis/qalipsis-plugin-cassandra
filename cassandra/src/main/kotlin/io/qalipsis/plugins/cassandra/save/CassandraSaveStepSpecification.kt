package io.qalipsis.plugins.cassandra.save

import io.qalipsis.api.annotations.Spec
import io.qalipsis.api.context.StepContext
import io.qalipsis.api.steps.AbstractStepSpecification
import io.qalipsis.api.steps.StepMonitoringConfiguration
import io.qalipsis.api.steps.StepSpecification
import io.qalipsis.plugins.cassandra.CassandraStepSpecification
import io.qalipsis.plugins.cassandra.configuration.CassandraServerConfiguration

/**
 * Specification for a [io.qalipsis.plugins.cassandra.save.CassandraSaveStep] to save records in Cassandra.
 *
 * The output is a list of [CassandraSaveResult].
 *
 * @author Svetlana Paliashchuk
 */
interface CassandraSaveStepSpecification<I> :
    StepSpecification<I, CassandraSaveResult<I>, CassandraSaveStepSpecification<I>>,
    CassandraStepSpecification<I, CassandraSaveResult<I>, CassandraSaveStepSpecification<I>> {

    /**
     * Configures connection to the Cassandra cluster.
     */
    fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit)

    /**
     * Configures the name of the table.
     */
    fun table(tableName: suspend (ctx: StepContext<*, *>, input: I) -> String)

    /**
     * Configures the columns names.
     */
    fun columns(columns: suspend (ctx: StepContext<*, *>, input: I) -> List<String>)

    /**
     * Defines the rows to be saved.
     */
    fun rows(rowsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<CassandraSaveRow>)

    /**
     * Configures the monitoring of the save step.
     */
    fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit)

}

/**
 * Implementation of [CassandraSaveStepSpecification].
 *
 * @author Svetlana Paliashchuk
 */
@Spec
internal class CassandraSaveStepSpecificationImpl<I> :
    CassandraSaveStepSpecification<I>,
    AbstractStepSpecification<I, CassandraSaveResult<I>, CassandraSaveStepSpecification<I>>() {

    internal var serversConfig = CassandraServerConfiguration()

    internal var columnsConfig: (suspend (ctx: StepContext<*, *>, input: I) -> List<String>) =  { _, _ -> emptyList() }

    internal var tableName: (suspend (ctx: StepContext<*, *>, input: I) -> String) =  { _, _ -> "" }

    internal var rowsFactory: (suspend (ctx: StepContext<*, *>, input: I) -> List<CassandraSaveRow>) =
        { _, _ -> emptyList() }

    internal var monitoringConfig = StepMonitoringConfiguration()

    override fun connect(serverConfiguration: CassandraServerConfiguration.() -> Unit) {
        this.serversConfig.serverConfiguration()
    }

    override fun columns(columns: suspend (ctx: StepContext<*, *>, input: I) -> List<String>){
        this.columnsConfig = columns
    }

    override fun table(tableName: (suspend (ctx: StepContext<*, *>, input: I) -> String)){
        this.tableName = tableName
    }

    override fun rows(rowsFactory: suspend (ctx: StepContext<*, *>, input: I) -> List<CassandraSaveRow>) {
        this.rowsFactory = rowsFactory
    }

    override fun monitoring(monitoringConfig: StepMonitoringConfiguration.() -> Unit) {
        this.monitoringConfig.monitoringConfig()
    }

}

/**
 * Saves data in Cassandra using a save query.
 *
 * @author Svetlana Paliashchuk
 */
fun <I> CassandraStepSpecification<*, I, *>.save(
    configurationBlock: CassandraSaveStepSpecification<I>.() -> Unit
): CassandraSaveStepSpecification<I> {
    val step = CassandraSaveStepSpecificationImpl<I>()
    step.configurationBlock()

    this.add(step)
    return step
}



