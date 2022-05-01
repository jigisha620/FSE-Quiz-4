package test.milk.start

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import com.rabbitmq.client.ConnectionFactory
import com.rabbitmq.client.MessageProperties
import io.ktor.http.*
import io.ktor.server.testing.*
import io.milk.products.PurchaseInfo
import io.milk.rabbitmq.BasicRabbitConfiguration
import io.milk.rabbitmq.RabbitTestSupport
import io.milk.start.module
import io.milk.testsupport.testDbPassword
import io.milk.testsupport.testDbUsername
import io.milk.testsupport.testJdbcUrl
import io.mockk.clearAllMocks
import org.junit.Before
import org.junit.Test
import test.milk.TestScenarioSupport
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class AppTest {
    private val testSupport = RabbitTestSupport()
    private val engine = TestApplicationEngine()
    private val mapper: ObjectMapper = ObjectMapper().registerKotlinModule()

    @Before
    fun before() {
        BasicRabbitConfiguration("products-exchange", "products").setUp()
        testSupport.purge("products")

        BasicRabbitConfiguration("products-exchange", "saferProductQueue").setUp()
        testSupport.purge("saferProductQueue")

        clearAllMocks()
        TestScenarioSupport().loadTestScenario("products")
        engine.start(wait = false)
        engine.application.module(testJdbcUrl, testDbUsername, testDbPassword)
    }

    @Test
    fun testIndex() {
        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                assertEquals(200, response.status()?.value)
                assertTrue(response.content!!.contains("milk"))
                assertTrue(response.content!!.contains("bacon"))
                assertTrue(response.content!!.contains("tuna"))
                assertTrue(response.content!!.contains("eggs"))
                assertTrue(response.content!!.contains("kombucha"))
                assertTrue(response.content!!.contains("apples"))
                assertTrue(response.content!!.contains("ice tea"))
                assertTrue(response.content!!.contains("yogurt"))
            }
        }
    }

    @Test
    fun testQuantity_1() {
        makePurchase(PurchaseInfo(105442, "milk", 1), "products")
        testSupport.waitForConsumers("products")

        with(engine) {
            with(handleRequest(io.ktor.http.HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("130"))
            }
        }
    }

    @Test
    fun testQuantity_50() {
        makePurchases(PurchaseInfo(105442, "milk", 1), "products")
        testSupport.waitForConsumers("products")

        with(engine) {
            with(handleRequest(io.ktor.http.HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("81"))
            }
        }
    }

    @Test
    fun testSaferQuantity() {
        // TODO -
        //  test a "safer" purchase, one where you are using a different "safer" queue
        //  then wait for consumers,
        //  then make a request
        //  and assert that the milk count 130
        makePurchase(PurchaseInfo(105442,"milk",1),"saferProductQueue")
        testSupport.waitForConsumers("saferProductQueue")
        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("130"))
            }
        }

    }

    @Test
    fun testBestCase() {
        makePurchases(PurchaseInfo(105443, "bacon", 1), "saferProductQueue")
        // TODO -
        //  uncomment the below after introducing the safer product update handler with manual acknowledgement
        testSupport.waitForConsumers("saferProductQueue")

        with(engine) {
            with(handleRequest(HttpMethod.Get, "/")) {
                assertTrue(response.content!!.contains("72"))
            }
        }
    }

    ///

    private fun makePurchase(purchase: PurchaseInfo, queue: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                val body = mapper.writeValueAsString(purchase).toByteArray()
                channel.basicPublish("", queue, MessageProperties.BASIC, body)
            }
        }
    }

    private fun makePurchases(purchase: PurchaseInfo, queue: String) {
        val factory = ConnectionFactory().apply { useNio() }
        factory.newConnection().use { connection ->
            connection.createChannel().use { channel ->
                (1..50).map {
                    val body = mapper.writeValueAsString(purchase).toByteArray()
                    channel.basicPublish("", queue, MessageProperties.PERSISTENT_BASIC, body)
                }
            }
        }
    }
}