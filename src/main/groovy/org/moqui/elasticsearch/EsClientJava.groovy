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
import org.elasticsearch.client.Client
import org.elasticsearch.index.query.QueryBuilder
import org.elasticsearch.index.reindex.DeleteByQueryAction
import org.elasticsearch.index.reindex.DeleteByQueryRequestBuilder
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextFactoryImpl
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@CompileStatic
class EsClientJava implements EsClient {
    protected final static Logger logger = LoggerFactory.getLogger(EsClientJava.class)

    protected Client client
    protected ExecutionContextFactoryImpl ecfi

    EsClientJava(Client elasticSearchClient, ExecutionContextFactoryImpl ecfi) {
        this.client = elasticSearchClient
        this.ecfi = ecfi
    }

    @Override
    boolean checkIndexExists(String indexName) {
        if (client.admin().indices().prepareAliasesExist(indexName).get().exists) return true
        return client.admin().indices().prepareExists(indexName).get().exists
    }

    @Override
    synchronized void checkCreateIndex(String indexName) {
        // if the index alias exists call it good
        if (client.admin().indices().prepareAliasesExist(indexName).get().exists) return

        EntityList ddList = ecfi.entityFacade.find("moqui.entity.document.DataDocument").condition("indexName", indexName).list()
        for (EntityValue dd in ddList) storeIndexAndMapping(indexName, dd)
    }

    @Override
    synchronized void checkCreateDocIndex(String dataDocumentId) {
        String idxName = ElasticSearchUtil.ddIdToEsIndex(dataDocumentId)
        if (client.admin().indices().prepareExists(idxName).get().exists) return

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
        client.admin().indices().prepareCreate(indexName).addMapping(docType, docMapping).get()
    }

    protected void storeIndexAndMapping(String indexName, EntityValue dd) {
        String dataDocumentId = (String) dd.getNoCheckSimple("dataDocumentId")
        String esIndexName = ElasticSearchUtil.ddIdToEsIndex(dataDocumentId)
        boolean hasIndex = client.admin().indices().prepareExists(esIndexName).get().exists
        // logger.warn("========== Checking index ${esIndexName} with alias ${indexName} , hasIndex=${hasIndex}")
        Map docMapping = ElasticSearchUtil.makeElasticSearchMapping(dataDocumentId, ecfi.getEci())
        if (hasIndex) {
            logger.info("Updating ElasticSearch index ${esIndexName} for ${dataDocumentId} with alias ${indexName} document mapping")
            client.admin().indices().preparePutMapping(esIndexName).setType(dataDocumentId).setSource(docMapping).get()
        } else {
            logger.info("Creating ElasticSearch index ${esIndexName} for ${dataDocumentId} with alias ${indexName} and adding document mapping")
            client.admin().indices().prepareCreate(esIndexName).addMapping(dataDocumentId, docMapping).get()
            // logger.warn("========== Added mapping for ${dataDocumentId} to index ${esIndexName}:\n${docMapping}")
            // add an alias (indexName) for the index (dataDocumentId.toLowerCase())
            client.admin().indices().prepareAliases().addAlias(esIndexName, indexName).get()
        }
    }

    @Override BulkResponse bulk(BulkRequest bulkRequest) { return client.bulk(bulkRequest).actionGet() }
    @Override GetResponse get(GetRequest getRequest) { return client.get(getRequest).actionGet() }
    @Override IndexResponse index(IndexRequest indexRequest) { return client.index(indexRequest).actionGet() }
    @Override UpdateResponse update(UpdateRequest updateRequest) { return client.update(updateRequest).actionGet() }
    @Override DeleteResponse delete(DeleteRequest deleteRequest) { return client.delete(deleteRequest).actionGet() }
    @Override SearchResponse search(SearchRequest searchRequest) { return client.search(searchRequest).actionGet() }

    @Override
    long deleteByQuery(String indexName, String documentType, QueryBuilder filter) {
        DeleteByQueryRequestBuilder req = new DeleteByQueryRequestBuilder(client, DeleteByQueryAction.INSTANCE)
        req.source().setIndices(indexName).setQuery(filter)
        if (documentType) req.source().setTypes(documentType)
        return req.get().getDeleted()
    }

    @Override Object getClient() { return client }
}
