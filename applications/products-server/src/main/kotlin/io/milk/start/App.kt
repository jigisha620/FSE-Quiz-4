package io.milk.start

import freemarker.cache.ClassTemplateLoader
import io.ktor.application.*
import io.ktor.features.*
import io.ktor.freemarker.*
import io.ktor.http.content.*
import io.ktor.jackson.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.jetty.*
import io.milk.database.createDatasource
import io.milk.products.ProductDataGateway
import io.milk.products.ProductService
import io.milk.rabbitmq.BasicRabbitConfiguration
import io.milk.rabbitmq.BasicRabbitListener
import java.util.*

fun Application.module(jdbcUrl: String, username: String, password: String) {
    val dataSource = createDatasource(jdbcUrl, username, password)
    val productService = ProductService(ProductDataGateway(dataSource))

    install(DefaultHeaders)
    install(CallLogging)
    install(FreeMarker) {
        templateLoader = ClassTemplateLoader(this::class.java.classLoader, "templates")
    }
    install(ContentNegotiation) {
        jackson()
    }
    install(Routing) {
        get("/") {
            val products = productService.findAll()
            call.respond(FreeMarkerContent("index.ftl", mapOf("products" to products)))
        }
        static("images") { resources("images") }
        static("style") { resources("style") }
    }

    BasicRabbitConfiguration(exchange = "products-exchange", queue = "products").setUp()
    BasicRabbitListener(
        queue = "products",
        delivery = ProductUpdateHandler(productService),
        cancel = ProductUpdateCancelHandler()
    ).start()
    
    // TODO -
    //  set up the rabbit configuration for your safer queue and
    //  start the rabbit listener with the safer product update handler with manual acknowledgement
    //  this looks similar to the above invocation

    BasicRabbitConfiguration(exchange = "products-exchange", queue = "saferProductQueue").setUp()
    BasicRabbitListener(
        queue = "saferProductQueue",
        delivery = ProductUpdateHandler(productService),
        cancel = ProductUpdateCancelHandler(),
        autoAck = false
    ).start()

    

}

fun main() {
    TimeZone.setDefault(TimeZone.getTimeZone("UTC"))
    val port = System.getenv("PORT")?.toInt() ?: 8081
    val jdbcUrl = System.getenv("JDBC_DATABASE_URL")
    val username = System.getenv("JDBC_DATABASE_USERNAME")
    val password = System.getenv("JDBC_DATABASE_USERNAME")

    embeddedServer(Jetty, port, module = { module(jdbcUrl, username, password) }).start()
}
