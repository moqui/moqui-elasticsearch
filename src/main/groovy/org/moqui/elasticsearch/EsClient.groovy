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
import org.elasticsearch.index.query.QueryBuilder

@CompileStatic
interface EsClient {
    boolean checkIndexExists(String indexName)
    void checkCreateIndex(String indexName)
    void checkCreateDocIndex(String dataDocumentId)
    void putIndexMappings(String indexName)
    void createIndex(String indexName, String docType, Map docMapping)

    BulkResponse bulk(BulkRequest bulkRequest)
    GetResponse get(GetRequest getRequest)
    IndexResponse index(IndexRequest indexRequest)
    UpdateResponse update(UpdateRequest updateRequest)
    DeleteResponse delete(DeleteRequest deleteRequest)
    SearchResponse search(SearchRequest searchRequest)
    long deleteByQuery(String indexName, String documentType, QueryBuilder filter)

    Object getClient()
}
