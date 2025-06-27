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