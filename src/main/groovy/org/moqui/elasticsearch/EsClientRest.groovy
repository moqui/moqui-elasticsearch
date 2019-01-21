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
import org.apache.http.HttpEntity
import org.apache.http.entity.ContentType
import org.apache.http.nio.entity.NStringEntity
import org.elasticsearch.action.admin.indices.alias.Alias
import org.elasticsearch.action.admin.indices.alias.get.GetAliasesRequest
import org.elasticsearch.action.admin.indices.create.CreateIndexRequest
import org.elasticsearch.action.admin.indices.get.GetIndexRequest
import org.elasticsearch.action.admin.indices.mapping.put.PutMappingRequest
import org.elasticsearch.action.bulk.BulkRequest
import org.elasticsearch.action.bulk.BulkResponse
import org.elasticsearch.action.delete.DeleteRequest
import org.elasticsearch.action.delete.DeleteResponse
import org.elasticsearch.action.get.GetRequest
import org.elasticsearch.action.get.GetResponse
import org.elasticsearch.action.index.IndexRequest
import org.elasticsearch.action.index.IndexResponse
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.action.update.UpdateRequest
import org.elasticsearch.action.update.UpdateResponse
import org.elasticsearch.client.Response
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.xcontent.XContentHelper
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilder
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@CompileStatic
class EsClientRest implements EsClient {
    protected final static Logger logger = LoggerFactory.getLogger(EsClientRest.class)

    protected RestHighLevelClient client
    protected ExecutionContextFactoryImpl ecfi

    EsClientRest(RestHighLevelClient client, ExecutionContextFactoryImpl ecfi) {
        this.client = client
        this.ecfi = ecfi
    }

    @Override
    boolean checkIndexExists(String indexName) {
        if (client.indices().existsAlias(new GetAliasesRequest(indexName))) return true
        return client.indices().exists(new GetIndexRequest().indices(indexName))
    }

    @Override
    synchronized void checkCreateIndex(String indexName) {
        // if the index alias exists call it good
        if (client.indices().exists(new GetIndexRequest().indices(indexName))) return

        EntityList ddList = ecfi.entityFacade.find("moqui.entity.document.DataDocument").condition("indexName", indexName).list()
        for (EntityValue dd in ddList) storeIndexAndMapping(indexName, dd)
    }

    @Override
    synchronized void checkCreateDocIndex(String dataDocumentId) {
        String idxName = ElasticSearchUtil.ddIdToEsIndex(dataDocumentId)
        if (client.indices().exists(new GetIndexRequest().indices(idxName))) return

        EntityValue dd = ecfi.entityFacade.find("moqui.entity.document.DataDocument").condition("dataDocumentId", dataDocumentId).one()
        storeIndexAndMapping((String) dd.indexName, dd)
    }

    @Override
    void putIndexMappings(String indexName) {
        EntityList ddList = ecfi.entityFacade.find("moqui.entity.document.DataDocument").condition("indexName", indexName).list()
        for (EntityValue dd in ddList) storeIndexAndMapping(indexName, dd)
    }

    @Override
    void createIndex(String indexName, String docType, Map docMapping) {
        client.indices().create(new CreateIndexRequest(indexName).mapping(docType, docMapping))
    }

    protected void storeIndexAndMapping(String indexName, EntityValue dd) {
        String dataDocumentId = (String) dd.getNoCheckSimple("dataDocumentId")
        String esIndexName = ElasticSearchUtil.ddIdToEsIndex(dataDocumentId)

        boolean hasIndex = client.indices().exists(new GetIndexRequest().indices(esIndexName))
        // logger.warn("========== Checking index ${esIndexName} with alias ${indexName} , hasIndex=${hasIndex}")
        Map docMapping = ElasticSearchUtil.makeElasticSearchMapping(dataDocumentId, ecfi.getEci())
        if (hasIndex) {
            logger.info("Updating ElasticSearch index ${esIndexName} for ${dataDocumentId} with alias ${indexName} document mapping")
            client.indices().putMapping(new PutMappingRequest(esIndexName).type(dataDocumentId).source(docMapping))
        } else {
            logger.info("Creating ElasticSearch index ${esIndexName} for ${dataDocumentId} with alias ${indexName} and adding document mapping")
            client.indices().create(new CreateIndexRequest(esIndexName).mapping(dataDocumentId, docMapping).alias(new Alias(indexName)))
            // logger.warn("========== Added mapping for ${dataDocumentId} to index ${esIndexName}:\n${docMapping}")
        }
    }

    @Override BulkResponse bulk(BulkRequest bulkRequest) { return client.bulk(bulkRequest) }
    @Override GetResponse get(GetRequest getRequest) { return client.get(getRequest) }
    @Override IndexResponse index(IndexRequest indexRequest) { return client.index(indexRequest) }
    @Override UpdateResponse update(UpdateRequest updateRequest) { return client.update(updateRequest) }
    @Override DeleteResponse delete(DeleteRequest deleteRequest) { return client.delete(deleteRequest) }
    @Override SearchResponse search(SearchRequest searchRequest) { return client.search(searchRequest) }

    @Override
    long deleteByQuery(String indexName, String documentType, QueryBuilder filter) {
        if (filter == null) return 0

        String queryJson = "{ \"query\": " + filter.toString() + " }"
        StringBuilder pathBuilder = new StringBuilder()
        pathBuilder.append('/').append(indexName)
        if (documentType) pathBuilder.append('/').append(documentType)
        pathBuilder.append("/_delete_by_query")

        // logger.warn("deleteByQuery ${pathBuilder}:\n${queryJson}")
        HttpEntity entity = new NStringEntity(queryJson, ContentType.APPLICATION_JSON)
        Response response = client.lowLevelClient.performRequest("POST", pathBuilder.toString(), new HashMap<String, String>(), entity)

        InputStream is = response.getEntity().getContent()
        try {
            Map<String, Object> map = XContentHelper.convertToMap(XContentType.JSON.xContent(), is, true)
            // logger.warn("deleteByQuery response ${map}")
            long deleted = map.get("deleted") as long
            return deleted
        } finally {
            is.close()
        }

        /*
        DeleteByQueryRequestBuilder req = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
        req.source().setIndices(indexName).setQuery(filter)
        if (documentType) req.source().setTypes(documentType)
        return req.get().getDeleted()
        */
    }

    @Override Object getClient() { return client }
}
