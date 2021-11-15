package io.qalipsis.plugins.cassandra

import io.qalipsis.api.scenario.ScenarioSpecification
import io.qalipsis.api.steps.AbstractPluginStepWrapper
import io.qalipsis.api.steps.AbstractScenarioSpecificationWrapper
import io.qalipsis.api.steps.StepSpecification


/**
 * Interface of a Cassandra step to define it in the appropriate step specifications namespace.
 *
 * @author Maxim Golokhov
 */
interface CassandraStepSpecification<INPUT, OUTPUT, SELF : StepSpecification<INPUT, OUTPUT, SELF>> :
    StepSpecification<INPUT, OUTPUT, SELF>

/**
 * Step wrapper to enter the namespace for the Cassandra step specifications.
 *
 * @author Maxim Golokhov
 */
class CassandraStepSpecificationImpl<INPUT, OUTPUT>(wrappedStepSpec: StepSpecification<INPUT, OUTPUT, *>) :
    AbstractPluginStepWrapper<INPUT, OUTPUT>(wrappedStepSpec),
    CassandraStepSpecification<INPUT, OUTPUT, AbstractPluginStepWrapper<INPUT, OUTPUT>>

fun <INPUT, OUTPUT> StepSpecification<INPUT, OUTPUT, *>.cassandra(): CassandraStepSpecification<INPUT, OUTPUT, *> =
    CassandraStepSpecificationImpl(this)

/**
 * Scenario wrapper to enter the namespace for the Cassandra step specifications.
 *
 * @author Maxim Golokhov
 */
class CassandraNamespaceScenarioSpecification(scenario: ScenarioSpecification) :
    AbstractScenarioSpecificationWrapper(scenario)

fun ScenarioSpecification.cassandra() = CassandraNamespaceScenarioSpecification(this)