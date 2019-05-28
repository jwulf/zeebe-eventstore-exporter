package io.zeebe

import io.zeebe.exporter.api.context.Context
import io.zeebe.exporter.api.context.Controller
import io.zeebe.exporter.api.record.Record
import io.zeebe.exporter.api.spi.Exporter
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.CloseableHttpClient
import org.apache.http.impl.client.HttpClients
import org.json.JSONArray
import org.json.JSONObject
import org.slf4j.Logger
import org.apache.commons.lang3.tuple.ImmutablePair

import java.io.IOException
import java.time.Duration
import java.util.*

class EventStoreExporter : Exporter {

    private lateinit var log: Logger
    private lateinit var controller: Controller

    private var batchSize: Int = 0
    private var batchExecutionTimer: Duration? = null
    private var url: String? = null
    private var batchTimerMilli: Int = 0
    private val queue = LinkedList<ImmutablePair<Long, JSONObject>>()

    override fun configure(context: Context) {
        log = context.logger
        val configuration = context
                .configuration
                .instantiate<EventStoreExporterConfiguration>(EventStoreExporterConfiguration::class.java)

        applyEnvironmentVariables(configuration)

        batchSize = configuration.batchSize
        batchTimerMilli = configuration.batchTimeMilli
        url = configuration.url + "/streams/" + configuration.streamName

        log!!.debug("Exporter configured with {}", configuration)

        // @TODO
        // Depends on https://github.com/zeebe-io/zeebe/issues/2546
        // sendBatchToEventStore(new JSONArray());
    }

    override fun open(controller: Controller) {
        this.controller = controller
        if (batchTimerMilli > 0) {
            batchExecutionTimer = Duration.ofMillis(batchTimerMilli.toLong())
            scheduleNextBatch()
        }
    }

    private fun scheduleNextBatch() {
        controller.scheduleTask(batchExecutionTimer) { this.batchRecordsFromQueue() }
    }

    @Throws(IOException::class)
    private fun sendBatchToEventStore(jsonArray: JSONArray) {
        val client = HttpClients.createDefault()
        val httpPost = HttpPost(url)
        val json = StringEntity(jsonArray.toString())
        httpPost.entity = json
        httpPost.setHeader("Content-Type", MIME_TYPE)
        client.execute(httpPost)
        client.close()
    }

    private fun batchRecordsFromQueue() {
        if (queue.isEmpty()) {
            scheduleNextBatch()
            return
        }

        var lastRecordSentPosition: Long = -1L
        val countRecordsToSend = Math.max(batchSize, queue.size)

        val jsonArray = JSONArray()

        for (i in 0 until countRecordsToSend) {
            val current = queue.pollFirst()
            if (current != null) {
                jsonArray.put(current.value)
                lastRecordSentPosition = current.key
            }
        }

        try {
            sendBatchToEventStore(jsonArray)
            if (lastRecordSentPosition != - 1L) {
                controller.updateLastExportedRecordPosition(lastRecordSentPosition)
            }
        } catch (e: IOException) {
            log.debug(e.message)
        } finally {
            scheduleNextBatch()
        }
    }

    override fun close() {
        log.debug("Closing Event Store Exporter")
    }

    override fun export(record: Record<*>) {
        val json = JSONObject()
        json.put("eventId", UUID.randomUUID())
        json.put("data", JSONObject(record.toJson()))
        json.put("eventType", "ZeebeEvent")
        queue.add(ImmutablePair(record.position, json))
    }

    private fun applyEnvironmentVariables(configuration: EventStoreExporterConfiguration) {
        val environment = System.getenv()

        Optional.ofNullable(environment[ENV_STREAM_NAME])
                .ifPresent { streamName -> configuration.streamName = streamName }
        Optional.ofNullable(environment[ENV_URL])
                .ifPresent { url -> configuration.url = url }
    }

    companion object {
        private const val ENV_PREFIX = "EVENT_STORE_EXPORTER_"
        private const val ENV_URL = ENV_PREFIX + "URL"
        private const val ENV_STREAM_NAME = ENV_PREFIX + "STREAM_NAME"
        private const val MIME_TYPE = "application/vnd.eventstore.events+json"
    }
}
