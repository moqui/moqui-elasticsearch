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
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
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
class ElasticSearchRestToolFactory implements ToolFactory<EsClient> {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticSearchRestToolFactory.class)
    final static String TOOL_NAME = "ElasticSearch"

    protected ExecutionContextFactory ecf = null

    /** ElasticSearch Client */
    protected RestHighLevelClient restClient
    /** ES Client Wrapper */
    protected EsClientRest esClient

    /** Default empty constructor */
    ElasticSearchRestToolFactory() { }

    @Override
    String getName() { return TOOL_NAME }
    @Override
    void init(ExecutionContextFactory ecf) {
        this.ecf = ecf

        ArrayList<HttpHost> hostList = new ArrayList<>()
        for (int i = 1; i < 10; i++) {
            String propVal = System.getProperty("elasticsearch_host" + i)
            if (propVal) hostList.add(HttpHost.create(propVal))
        }

        logger.info("Initializing ElasticSearch RestHighLevelClient with hosts: ${hostList}")

        restClient = new RestHighLevelClient(RestClient.builder(hostList.toArray(new HttpHost[0])))
        esClient = new EsClientRest(restClient, (ExecutionContextFactoryImpl) ecf)

        // Index DataFeed with indexOnStartEmpty=Y
        EntityList dataFeedList = ecf.entity.find("moqui.entity.feed.DataFeed")
                .condition("indexOnStartEmpty", "Y").disableAuthz().list()
        for (EntityValue dataFeed in dataFeedList) {
            Set<String> indexNames = new HashSet<>(ecf.entity.find("moqui.entity.feed.DataFeedDocumentDetail")
                    .condition("dataFeedId", dataFeed.dataFeedId).disableAuthz().list()*.getString("indexName"))
            boolean foundNotExists = false
            for (String indexName in indexNames) if (!esClient.checkIndexExists(indexName)) foundNotExists = true
            if (foundNotExists) {
                String jobRunId = ecf.service.job("IndexDataFeedDocuments").parameter("dataFeedId", dataFeed.dataFeedId).run()
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
    }

    ExecutionContextFactory getEcf() { return ecf }
}
