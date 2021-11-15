package io.qalipsis.plugins.cassandra

import io.qalipsis.api.steps.StepSpecification

/**
 * Interface of a step that provides a list of items by default but can be amended to flatten those lists.
 *
 * @author Maxim Golokhov
 */
interface Flattenable<T> : StepSpecification<Unit, List<T>, Flattenable<T>> {

    /**
     * Returns each record of a batch individually to the next steps.
     */
    fun flatten(): StepSpecification<Unit, T, *>

}
