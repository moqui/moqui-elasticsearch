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
import org.apache.http.HttpHost
import org.elasticsearch.client.Client
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.settings.Settings
import org.moqui.context.ExecutionContextFactory
import org.moqui.context.ToolFactory
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/** ElasticSearch Client is used for indexing and searching documents */
/** NOTE: embedded ElasticSearch may soon go away, see: https://www.elastic.co/blog/elasticsearch-the-server */
@CompileStatic
class ElasticSearchToolFactory implements ToolFactory<EsClient> {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticSearchToolFactory.class)
    final static String TOOL_NAME = "ElasticSearch"

    protected ExecutionContextFactory ecf = null

    /** ElasticSearch Node */
    protected org.elasticsearch.node.Node elasticSearchNode
    /** ElasticSearch Embedded Client */
    protected Client elasticSearchClient
    /** ElasticSearch REST Client */
    protected RestHighLevelClient restClient
    /** ES Client Wrapper */
    protected EsClient esClient

    /** Default empty constructor */
    ElasticSearchToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }
    @Override
    void init(ExecutionContextFactory ecf) {
        this.ecf = ecf

        String esMode = System.getProperty("elasticsearch_mode")
        if (esMode == "rest") {
            ArrayList<HttpHost> hostList = new ArrayList<>()
            for (int i = 1; i < 10; i++) {
                String propVal = System.getProperty("elasticsearch_host" + i)
                if (propVal) hostList.add(HttpHost.create(propVal))
            }

            logger.info("Initializing ElasticSearch RestHighLevelClient with hosts: ${hostList}")

            restClient = new RestHighLevelClient(RestClient.builder(hostList.toArray(new HttpHost[0])))
            esClient = new EsClientRest(restClient, (ExecutionContextFactoryImpl) ecf)
        } else {
            // set the ElasticSearch home (for config, modules, plugins, scripts, etc), data, and logs directories
            // see https://www.elastic.co/guide/en/elasticsearch/reference/current/setup-dir-layout.html
            // NOTE: could use getPath() instead of toExternalForm().substring(5) for file specific URLs, will work on Windows?
            String pathHome = ecf.resource.getLocationReference("component://moqui-elasticsearch/home").getUrl().toExternalForm().substring(5)
            String pathData = ecf.runtimePath + "/elasticsearch/data"
            String pathLogs = ecf.runtimePath + "/log"
            logger.info("Starting ElasticSearch, home at ${pathHome}, data at ${pathData}, logs at ${pathLogs}")

            // some code to cleanup the classpath, avoid jar hell IllegalStateException
            String initialClassPath = System.getProperty("java.class.path")
            StringBuilder newClassPathSb = new StringBuilder()
            String pathSeparator = System.getProperty("path.separator")
            Set<String> cpEntrySet = new HashSet<>()
            if (initialClassPath) for (String cpEntry in initialClassPath.split(pathSeparator)) {
                if (!cpEntry) {
                    logger.warn("Found empty classpath entry, removing as ElasticSearch jar hell will blow up")
                    continue
                }
                if (cpEntrySet.contains(cpEntry)) {
                    logger.warn("Found duplicate classpath entry ${cpEntry}, removing as ElasticSearch jar hell will blow up")
                    continue
                }
                cpEntrySet.add(cpEntry)
                if (newClassPathSb.length() > 0) newClassPathSb.append(pathSeparator)
                newClassPathSb.append(cpEntry)
            }
            System.setProperty("java.class.path", newClassPathSb.toString())
            // logger.info("Before ElasticSearch java.class.path: ${System.getProperty('java.class.path')}")

            // build the ES node
            Settings.Builder settings = Settings.builder()
            settings.put("path.home", pathHome)
            settings.put("path.data", pathData)
            settings.put("path.logs", pathLogs)

            // arbitrary elasticsearch config, always starts with `es_config.`, e.g `es_config.node.name` will set `node.name` in setting
            for (String propName in System.getProperties().stringPropertyNames()) {
                if (!propName.startsWith("es_config.") || !System.getProperty(propName)) continue
                String esConfigName = propName.substring(10)
                if (esConfigName) settings.put(esConfigName, System.getProperty(propName))
            }
            // do env vars after Java system properties so they override
            for (String envName in System.getenv().keySet()) {
                if (!envName.startsWith("es_config.")) continue
                String esConfigName = envName.substring(10)
                if (esConfigName) settings.put(esConfigName, System.getenv(envName))
            }

            elasticSearchNode = new org.elasticsearch.node.Node(settings.build())
            elasticSearchNode.start()
            elasticSearchClient = elasticSearchNode.client()
            esClient = new EsClientJava(elasticSearchClient, (ExecutionContextFactoryImpl) ecf)
        }

        // Index DataFeed with indexOnStartEmpty=Y
        EntityList dataFeedList = ecf.entity.find("moqui.entity.feed.DataFeed")
                .condition("indexOnStartEmpty", "Y").disableAuthz().list()
        for (EntityValue dataFeed in dataFeedList) {
            Set<String> indexNames = new HashSet<>(ecf.entity.find("moqui.entity.feed.DataFeedDocumentDetail")
                    .condition("dataFeedId", dataFeed.dataFeedId).disableAuthz().list()*.getString("indexName"))
            boolean foundNotExists = false
            for (String indexName in indexNames) if (!esClient.checkIndexExists(indexName)) foundNotExists = true
            if (foundNotExists) {
                // NOTE: called with localOnly(true) to avoid issues during startup if a distributed executor service is configured
                String jobRunId = ecf.service.job("IndexDataFeedDocuments").parameter("dataFeedId", dataFeed.dataFeedId).localOnly(true).run()
                logger.info("Found index does not exist for DataFeed ${dataFeed.dataFeedId}, started job ${jobRunId} to index")
            }
        }
    }
    @Override
    void preFacadeInit(ExecutionContextFactory ecf) { }

    @Override
    EsClient getInstance(Object... parameters) {
        if (esClient == null) throw new IllegalStateException("ElasticSearchToolFactory not initialized")
        return esClient
    }

    @Override
    void destroy() {
        if (restClient != null) try {
            restClient.close()
            logger.info("ElasticSearch closed")
        } catch (Throwable t) { logger.error("Error in ElasticSearch node close", t) }

        if (elasticSearchNode != null) try {
            elasticSearchNode.close()
            while (!elasticSearchNode.isClosed()) {
                logger.info("ElasticSearch still closing")
                this.wait(1000)
            }
            logger.info("ElasticSearch closed")
        } catch (Throwable t) { logger.error("Error in ElasticSearch node close", t) }
    }

    ExecutionContextFactory getEcf() { return ecf }
}
