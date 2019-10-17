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
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.transport.TransportException
import org.moqui.Moqui
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

import javax.servlet.*
import javax.servlet.http.HttpServletRequest
import javax.servlet.http.HttpServletResponse
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.TimeUnit

/** Save data about HTTP requests to ElasticSearch using a Servlet Filter */
@CompileStatic
class ElasticRequestLogFilter implements Filter {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticRequestLogFilter.class)
    final static String INDEX_NAME = "moqui_http_log"
    final static String DOC_TYPE = "MoquiHttpRequest"

    protected FilterConfig filterConfig = null
    protected ExecutionContextFactoryImpl ecfi = null

    private EsClient esClient = null
    private boolean disabled = false
    final ConcurrentLinkedQueue<Map> requestLogQueue = new ConcurrentLinkedQueue<>()

    ElasticRequestLogFilter() { super() }

    @Override
    void init(FilterConfig filterConfig) throws ServletException {
        this.filterConfig = filterConfig

        ecfi = (ExecutionContextFactoryImpl) filterConfig.servletContext.getAttribute("executionContextFactory")
        if (ecfi == null) ecfi = (ExecutionContextFactoryImpl) Moqui.executionContextFactory

        try {
            esClient = ecfi.getTool("ElasticSearch", EsClient.class)
        } catch (Throwable t) {
            logger.error("Error getting EsClient instance: ${t.toString()}")
        }
        if (esClient == null) {
            logger.error("In ElasticRequestLogFilter init could not find ElasticSearch tool")
        } else {
            // check for index exists, create with mapping for log doc if not
            boolean hasIndex = esClient.checkIndexExists(INDEX_NAME)
            if (!hasIndex) esClient.createIndex(INDEX_NAME, DOC_TYPE, docMapping)

            RequestLogQueueFlush rlqf = new RequestLogQueueFlush(this)
            ecfi.scheduledExecutor.scheduleAtFixedRate(rlqf, 15, 5, TimeUnit.SECONDS)
        }
    }

    // TODO: add geoip (see https://www.elastic.co/guide/en/logstash/current/plugins-filters-geoip.html)
    // TODO: add user_agent (see https://www.elastic.co/guide/en/logstash/current/plugins-filters-useragent.html)

    final static Map docMapping = [properties:[
            '@timestamp':[type:'date', format:'epoch_millis'], remote_ip:[type:'ip'], remote_user:[type:'keyword'],
            server_ip:[type:'keyword'], content_type:[type:'text'],
            request_method:[type:'keyword'], request_scheme:[type:'keyword'], request_host:[type:'keyword'],
            request_path:[type:'text'], request_query:[type:'text'], http_version:[type:'half_float'], response:[type:'short'],
            time_initial_ms:[type:'integer'], time_final_ms:[type:'integer'], bytes:[type:'long'],
            referrer:[type:'text'], agent:[type:'text']
        ]
    ]

    @Override
    void doFilter(ServletRequest req, ServletResponse resp, FilterChain chain) throws IOException, ServletException {
        long startTime = System.currentTimeMillis()

        if (esClient == null || disabled || !DispatcherType.REQUEST.is(req.getDispatcherType()) ||
                !(req instanceof HttpServletRequest) || !(resp instanceof HttpServletResponse)) {
            chain.doFilter(req, resp)
            return
        }

        HttpServletRequest request = (HttpServletRequest) req
        HttpServletResponse response = (HttpServletResponse) resp
        CountingHttpServletResponseWrapper responseWrapper = (CountingHttpServletResponseWrapper) null
        try {
            responseWrapper = new CountingHttpServletResponseWrapper(response)
        } catch (Exception e) {
            logger.warn("Error initializing CountingHttpServletResponseWrapper", e)
        }
        // chain first so response is run
        if (responseWrapper != null) {
            chain.doFilter(req, responseWrapper)
        } else {
            chain.doFilter(req, resp)
        }

        if (request.isAsyncStarted()) {
            request.getAsyncContext().addListener(new RequestLogAsyncListener(this, startTime), req, responseWrapper != null ? responseWrapper : response)
        } else {
            logRequest(request, responseWrapper != null ? responseWrapper : response, startTime)
        }
    }

    void logRequest(HttpServletRequest request, HttpServletResponse response, long startTime) {
        long initialTime = System.currentTimeMillis() - startTime
        // always flush the buffer so we can get the final time; this is for some reason NECESSARY for the wrapper otherwise content doesn't make it through
        response.flushBuffer()

        String clientIpAddress = request.getRemoteAddr()
        String forwardedFor = request.getHeader("X-Forwarded-For")
        if (forwardedFor != null && !forwardedFor.isEmpty()) clientIpAddress = forwardedFor.split(",")[0].trim()

        float httpVersion = 0.0
        String protocol = request.getProtocol().trim()
        int psIdx = protocol.indexOf("/")
        if (psIdx > 0) try { httpVersion = Float.parseFloat(protocol.substring(psIdx + 1)) } catch (Exception e) { }

        // get response size, only way to wrap the response with wrappers for Writer and OutputStream to count size? messy, slow...
        long written = 0L
        if (response instanceof CountingHttpServletResponseWrapper) written = ((CountingHttpServletResponseWrapper) response).getWritten()

        // final time after streaming response (ie flush response)
        long finalTime = System.currentTimeMillis() - startTime

        Map reqMap = ['@timestamp':startTime, remote_ip:clientIpAddress, remote_user:request.getRemoteUser(),
                server_ip:request.getLocalAddr(), content_type:response.getContentType(),
                request_method:request.getMethod(), request_scheme:request.getScheme(), request_host:request.getServerName(),
                request_path:request.getRequestURI(), request_query:request.getQueryString(), http_version:httpVersion,
                response:response.getStatus(), time_initial_ms:initialTime, time_final_ms:finalTime, bytes:written,
                referrer:request.getHeader("Referrer"), agent:request.getHeader("User-Agent")]
        requestLogQueue.add(reqMap)
        // logger.info("${request.getMethod()} ${request.getRequestURI()} - ${response.getStatus()} ${finalTime}ms ${written}b asyncs ${request.isAsyncStarted()}")
    }

    @Override void destroy() { }

    static class RequestLogQueueFlush implements Runnable {
        final static int maxCreates = 50
        final ElasticRequestLogFilter filter

        RequestLogQueueFlush(ElasticRequestLogFilter filter) { this.filter = filter }

        @Override synchronized void run() {
            while (filter.requestLogQueue.size() > 0) { flushQueue() }
        }
        void flushQueue() {
            final ConcurrentLinkedQueue<Map> queue = filter.requestLogQueue
            ArrayList<Map> createList = new ArrayList<>(maxCreates)
            int createCount = 0
            while (createCount < maxCreates) {
                Map message = queue.poll()
                if (message == null) break
                // increment the count and add the message
                createCount++
                createList.add(message)
            }
            int retryCount = 5
            while (retryCount > 0) {
                int createListSize = createList.size()
                if (createListSize == 0) break
                try {
                    // long startTime = System.currentTimeMillis()
                    try {
                        BulkRequest bulkRequest = new BulkRequest()
                        for (int i = 0; i < createListSize; i++) {
                            Map curMessage = createList.get(i)
                            // logger.warn(curMessage.toString())
                            bulkRequest.add(new IndexRequest(INDEX_NAME, DOC_TYPE, null).source(curMessage))
                        }
                        BulkResponse bulkResponse = filter.esClient.bulk(bulkRequest)
                        if (bulkResponse.hasFailures()) {
                            logger.error(bulkResponse.buildFailureMessage())
                        }
                    } catch (TransportException te) {
                        String message = te.getMessage()
                        if (message && message.toLowerCase().contains("stopped")) {
                            filter.disabled = true
                            logger.error("Stopping ElasticSearch HTTP Request logging, transport error: ${te.toString()}")
                        } else {
                            logger.error("Error  logging to ElasticSearch: ${te.toString()}")
                        }
                    } catch (Exception e) {
                        logger.error("Error logging to ElasticSearch: ${e.toString()}")
                    }
                    // logger.warn("Indexed ${createListSize} ElasticSearch log messages in ${System.currentTimeMillis() - startTime}ms")
                    break
                } catch (Throwable t) {
                    logger.error("Error indexing ElasticSearch log messages, retrying (${retryCount}): ${t.toString()}")
                    retryCount--
                }
            }
        }
    }

    static class RequestLogAsyncListener implements AsyncListener {
        ElasticRequestLogFilter filter
        private long startTime
        RequestLogAsyncListener(ElasticRequestLogFilter filter, long startTime) { this.filter = filter; this.startTime = startTime }

        @Override void onComplete(AsyncEvent event) throws IOException { logEvent(event) }
        @Override void onTimeout(AsyncEvent event) throws IOException { logEvent(event) }
        @Override void onError(AsyncEvent event) throws IOException { logEvent(event) }
        @Override void onStartAsync(AsyncEvent event) throws IOException { }

        void logEvent(AsyncEvent event) {
            if (event.getSuppliedRequest() instanceof HttpServletRequest && event.getSuppliedResponse() instanceof HttpServletResponse) {
                filter.logRequest((HttpServletRequest) event.getSuppliedRequest(), (HttpServletResponse) event.getSuppliedResponse(), startTime)
            }
        }
    }
}
