package io.qalipsis.plugins.cassandra.poll

import assertk.assertThat
import assertk.assertions.isFalse
import com.datastax.oss.driver.api.core.CqlSession
import com.datastax.oss.driver.api.core.CqlSessionBuilder
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.confirmVerified
import io.mockk.every
import io.mockk.spyk
import io.qalipsis.api.context.StepStartStopContext
import io.qalipsis.api.sync.SuspendedCountLatch
import io.qalipsis.plugins.cassandra.search.CassandraQueryClient
import io.qalipsis.test.coroutines.TestDispatcherProvider
import io.qalipsis.test.mockk.CleanMockkRecordedCalls
import io.qalipsis.test.mockk.coVerifyNever
import io.qalipsis.test.mockk.relaxedMockk
import io.qalipsis.test.mockk.verifyOnce
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import org.junit.jupiter.api.Assertions
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Timeout
import org.junit.jupiter.api.extension.RegisterExtension
import java.time.Duration


@CleanMockkRecordedCalls
internal class CassandraIterativeReaderTest {

    @JvmField
    @RegisterExtension
    val testDispatcherProvider = TestDispatcherProvider()

    val session = relaxedMockk<CqlSession>()

    val sessionBuilder = relaxedMockk<CqlSessionBuilder> {
        every { build() } returns session
    }

    private val cqlPollStatement = relaxedMockk<CqlPollStatement> {
        every { compose() } returns ("" to emptyList())
    }

    private val cassandraQueryClientImpl = relaxedMockk<CassandraQueryClient>()

    @Test
    @Timeout(5)
    @kotlinx.coroutines.ExperimentalCoroutinesApi
    fun `should not be running before start`() = testDispatcherProvider.runTest {
        // given
        val readerSpy = spyk(
            CassandraIterativeReader(
                this,
                sessionBuilder,
                cqlPollStatement,
                Duration.ofSeconds(1),
                cassandraQueryClientImpl
            )
        )

        //when there is no action

        //then
        assertThat(readerSpy.hasNext()).isFalse()
    }

    @Test
    @Timeout(5)
    fun `should poll after start`() = testDispatcherProvider.run {
        // given
        val countDownLatch = SuspendedCountLatch(3, true)
        val pollPeriod = Duration.ofMillis(50)
        val reader = spyk(CassandraIterativeReader(
            this,
            sessionBuilder,
            cqlPollStatement,
            pollPeriod,
            cassandraQueryClientImpl
        ) { Channel(Channel.UNLIMITED) }, recordPrivateCalls = true
        )
        coEvery { reader["poll"](any<CqlSession>(), any<StepStartStopContext>()) } coAnswers { countDownLatch.decrement() }


        // when
        reader.start(relaxedMockk())

        // then
        Assertions.assertTrue(reader.hasNext())
        countDownLatch.await()
        verifyOnce { cqlPollStatement.reset() }
        coVerify(atLeast = 3) {
            reader["poll"](any<CqlSession>(), any<StepStartStopContext>())
        }
        confirmVerified(cqlPollStatement)
    }

    @Test
    @Timeout(5)
    fun `should be stoppable`() = testDispatcherProvider.run {
        // given
        val countDownLatch = SuspendedCountLatch(3, true)
        val pollPeriod = Duration.ofMillis(50)
        val reader = spyk(CassandraIterativeReader(
            this,
            sessionBuilder,
            cqlPollStatement,
            pollPeriod,
            cassandraQueryClientImpl
        ) { Channel(Channel.UNLIMITED) }, recordPrivateCalls = true
        )

        coEvery { reader["poll"](any<CqlSession>(), any<StepStartStopContext>()) } coAnswers { countDownLatch.decrement() }


        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch.await()
        verifyOnce { cqlPollStatement.reset() }
        verifyOnce { sessionBuilder.build() }
        coVerify(atLeast = 3) {
            reader["poll"](any<CqlSession>(), any<StepStartStopContext>())
        }
        clearMocks(reader, cqlPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

        // then
        verifyOnce { cqlPollStatement.reset() }
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        coVerifyNever { reader["poll"](any<CqlSession>(), any<StepStartStopContext>()) }
    }

    @Test
    @Timeout(10)
    fun `should be restartable`() = testDispatcherProvider.run {
        // given
        val pollPeriod = Duration.ofMillis(50)
        // Count down for the first period of activity.
        val countDownLatch1 = SuspendedCountLatch(3)
        // Count down for the second period of activity.
        val countDownLatch2 = SuspendedCountLatch(3, true)

        val reader = spyk(
            CassandraIterativeReader(
                this,
                sessionBuilder,
                cqlPollStatement,
                pollPeriod,
                cassandraQueryClientImpl
            ) { Channel(Channel.UNLIMITED) }, recordPrivateCalls = true
        )
            coEvery { reader["poll"](any<CqlSession>(), any<StepStartStopContext>()) } coAnswers {
                if (countDownLatch1.get() > 0) {
                    countDownLatch1.decrement()
                } else {
                    countDownLatch2.decrement()
                }
            }



        // when
        reader.start(relaxedMockk())

        // then
        verifyOnce { cqlPollStatement.reset() }
        countDownLatch1.await()
        coVerify(atLeast = 3) {
            reader["poll"](any<CqlSession>(), any< StepStartStopContext>())
        }
        clearMocks(reader, cqlPollStatement, answers = false)

        // when
        reader.stop(relaxedMockk())

        // then
        verifyOnce { cqlPollStatement.reset() }
        verifyOnce { sessionBuilder.build() }
        Assertions.assertFalse(reader.hasNext())
        delay(200)
        coVerifyNever { reader["poll"](any<CqlSession>(), any< StepStartStopContext>()) }
        clearMocks(reader, cqlPollStatement, sessionBuilder, answers = false)

        // when
        reader.start(relaxedMockk())

        // then
        countDownLatch2.await()
        verifyOnce { cqlPollStatement.reset() }
        verifyOnce { sessionBuilder.build() }
        coVerify(atLeast = 3) {
            reader["poll"](any<CqlSession>(), any< StepStartStopContext>())
        }
    }
}
