/*
 * This software is in the public domain under CC0 1.0 Universal plus a 
 * Grant of Patent License.
 * 
 * To the extent possible under law, the author(s) have dedicated all
 * copyright and related and neighboring rights to this software to the
 * public domain worldwide. This software is distributed without any
 * warranty.
 * 
 * You should have received a copy of the CC0 Public Domain Dedication
 * along with this software (see the LICENSE.md file). If not, see
 * <http://creativecommons.org/publicdomain/zero/1.0/>.
 */
package org.moqui.elasticsearch

import groovy.transform.CompileStatic
import org.apache.logging.log4j.Level
import org.apache.logging.log4j.core.LogEvent
import org.apache.logging.log4j.core.layout.JsonLayout
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.action.bulk.BulkRequestBuilder
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.client.Client
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.transport.TransportException
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.LogEventSubscriber
import org.moqui.context.ToolFactory
import org.moqui.impl.context.ExecutionContextFactoryImpl

import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

@CompileStatic
class ElasticSearchLoggerToolFactory implements ToolFactory<LogEventSubscriber> {
    final static String TOOL_NAME = "ElasticSearchLogger"
    // TODO: make this configurable somehow
    final static String INDEX_NAME = "moqui_logs"
    final static String DOC_TYPE = "LogMessage"

    protected ExecutionContextFactoryImpl ecfi = null
    protected ElasticSearchSubscriber subscriber = null

    private Client elasticSearchClient = null
    private boolean disabled = false
    final ConcurrentLinkedQueue<String> logMessageQueue = new ConcurrentLinkedQueue<String>()

    /** Default empty constructor */
    ElasticSearchLoggerToolFactory() { }

    @Override String getName() { return TOOL_NAME }
    @Override void init(ExecutionContextFactory ecf) {
        ecfi = (ExecutionContextFactoryImpl) ecf

        elasticSearchClient = ecfi.getTool("ElasticSearch", Client.class)
        if (elasticSearchClient == null) {
            System.err.println("In ElasticSearchLogger init could not find ElasticSearch tool")
        } else {
            // check for index exists, create with mapping for log doc if not
            boolean hasIndex = elasticSearchClient.admin().indices().exists(new IndicesExistsRequest(INDEX_NAME)).actionGet().exists
            if (!hasIndex) {
                CreateIndexRequestBuilder cirb = elasticSearchClient.admin().indices().prepareCreate(INDEX_NAME)
                cirb.addMapping(DOC_TYPE, docMapping)
                cirb.execute().actionGet()
            }

            subscriber = new ElasticSearchSubscriber(this)
            ecfi.registerLogEventSubscriber(subscriber)

            LogMessageQueueFlush lmqf = new LogMessageQueueFlush(this)
            ecfi.scheduledExecutor.scheduleAtFixedRate(lmqf, 5, 5, TimeUnit.SECONDS)
        }

    }
    @Override void preFacadeInit(ExecutionContextFactory ecf) { }

    @Override LogEventSubscriber getInstance(Object... parameters) { return subscriber }
    @Override void destroy() {
        disabled = true
    }

    static class ElasticSearchSubscriber implements LogEventSubscriber {
        private final ElasticSearchLoggerToolFactory factory
        private JsonLayout layout

        ElasticSearchSubscriber(ElasticSearchLoggerToolFactory factory) {
            this.factory = factory
            JsonLayout.Builder builder = JsonLayout.newBuilder().setIncludeStacktrace(true)
            builder.setCompact(true).setComplete(false)
            layout = builder.build()
        }
        @Override
        void process(LogEvent event) {
            if (factory.disabled || factory.elasticSearchClient == null) return
            // NOTE: levels configurable in log4j2.xml but always exclude these
            if (Level.DEBUG.is(event.level) || Level.TRACE.is(event.level)) return
            factory.logMessageQueue.add(layout.toSerializable(event))
        }
    }

    static class LogMessageQueueFlush implements Runnable {
        final static int maxCreates = 20
        final ElasticSearchLoggerToolFactory factory

        LogMessageQueueFlush(ElasticSearchLoggerToolFactory factory) { this.factory = factory }

        @Override synchronized void run() {
            while (factory.logMessageQueue.size() > 0) {
                flushQueue()
            }
        }
        void flushQueue() {
            final ConcurrentLinkedQueue<String> queue = factory.logMessageQueue
            ArrayList<String> createList = new ArrayList<>(maxCreates)
            int createCount = 0
            while (createCount < maxCreates) {
                String message = queue.poll()
                if (message == null) break
                createCount++
                createList.add(message)
            }
            int retryCount = 5
            while (retryCount > 0) {
                try {
                    int createListSize = createList.size()
                    if (createListSize == 0) break
                    long startTime = System.currentTimeMillis()
                    try {
                        BulkRequestBuilder bulkBuilder = factory.elasticSearchClient.prepareBulk()
                        for (int i = 0; i < createListSize; i++) {
                            String curMessage = createList.get(i)
                            curMessage.replaceAll(/\\n/, "\\\\n")
                            // System.out.println(curMessage)
                            bulkBuilder.add(factory.elasticSearchClient.prepareIndex(INDEX_NAME, DOC_TYPE, null)
                                    .setSource(curMessage, XContentType.JSON))
                        }
                        BulkResponse bulkResponse = bulkBuilder.execute().actionGet()
                        if (bulkResponse.hasFailures()) {
                            System.out.println(bulkResponse.buildFailureMessage())
                        }
                    } catch (TransportException te) {
                        String message = te.getMessage()
                        if (message && message.toLowerCase().contains("stopped")) factory.disabled = true
                        System.out.println("Stopping ElasticSearch logging, transport error: ${te.toString()}")
                    } catch (Exception e) {
                        System.out.println("Error logging to ElasticSearch: ${e.toString()}")
                    }
                    System.out.println("Indexed ${createListSize} ElasticSearch log messages in ${System.currentTimeMillis() - startTime}ms")
                    break
                } catch (Throwable t) {
                    System.out.println("Error indexing ElasticSearch log messages, retrying (${retryCount}): ${t.toString()}")
                    retryCount--
                }
            }
        }
    }

    final static Map stackItemMapping = [type:'object', properties:[class:[type:'text'], method:[type:'text'], file:[type:'text'], line:[type:'long'],
            exact:[type:'boolean'], location:[type:'text'], version:[type:'keyword']]]
    final static Map docMapping = [properties:
            [timeMillis:[type:'date', format:'epoch_millis'], level:[type:'keyword'],
             thread:[type:'keyword'], threadId:[type:'long'], threadPriority:[type:'long'], endOfBatch:[type:'boolean'],
             loggerFqcn:[type:'keyword'], loggerName:[type:'text'], name:[type:'text'], message:[type:'text'],
             thrown:[type:'object', properties:[name:[type:'text'], message:[type:'text'], localizedMessage:[type:'text'], commonElementCount:[type:'long'],
                    extendedStackTrace:stackItemMapping, suppressed:stackItemMapping,
                    cause:[type:'object', properties:[name:[type:'text'], message:[type:'text'], localizedMessage:[type:'text'],
                            commonElementCount:[type:'long'], extendedStackTrace:stackItemMapping]]
            ]]
    ]]
}
