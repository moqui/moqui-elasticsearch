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

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import org.elasticsearch.action.admin.indices.create.CreateIndexRequestBuilder
import org.elasticsearch.action.admin.indices.exists.indices.IndicesExistsRequest
import org.elasticsearch.client.Client
import org.moqui.entity.EntityException
import org.moqui.entity.EntityList
import org.moqui.entity.EntityValue
import org.moqui.impl.context.ExecutionContextImpl
import org.moqui.impl.entity.EntityDefinition
import org.moqui.impl.entity.EntityJavaUtil
import org.moqui.impl.entity.FieldInfo
import org.slf4j.Logger
import org.slf4j.LoggerFactory

@CompileStatic
class ElasticSearchUtil {
    protected final static Logger logger = LoggerFactory.getLogger(ElasticSearchUtil.class)

    // NOTE: called in service scripts
    static void checkCreateIndex(String indexName, ExecutionContextImpl eci) {
        String baseIndexName = indexName.contains("__") ? indexName.substring(indexName.indexOf("__") + 2) : indexName

        Client client = (Client) eci.getTool("ElasticSearch", Client.class)
        boolean hasIndex = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().exists
        // logger.warn("========== Checking index ${indexName} (${baseIndexName}), hasIndex=${hasIndex}")
        if (hasIndex) return

        logger.info("Creating ElasticSearch index ${indexName} (${baseIndexName}) and adding document mappings")

        CreateIndexRequestBuilder cirb = client.admin().indices().prepareCreate(indexName)

        EntityList ddList = eci.entityFacade.find("moqui.entity.document.DataDocument").condition("indexName", baseIndexName).list()
        for (EntityValue dd in ddList) {
            Map docMapping = makeElasticSearchMapping((String) dd.dataDocumentId, eci)
            cirb.addMapping((String) dd.dataDocumentId, docMapping)
            // logger.warn("========== Added mapping for ${dd.dataDocumentId} to index ${indexName}:\n${docMapping}")

        }
        cirb.execute().actionGet()
    }

    // NOTE: called in service scripts
    static void putIndexMappings(String indexName, ExecutionContextImpl eci) {
        String baseIndexName = indexName.contains("__") ? indexName.substring(indexName.indexOf("__") + 2) : indexName

        Client client = (Client) eci.getTool("ElasticSearch", Client.class)
        boolean hasIndex = client.admin().indices().exists(new IndicesExistsRequest(indexName)).actionGet().isExists()
        if (!hasIndex) {
            client.admin().indices().prepareCreate(indexName).execute().actionGet()
        }

        EntityList ddList = eci.entity.find("moqui.entity.document.DataDocument").condition("indexName", baseIndexName).list()
        for (EntityValue dd in ddList) {
            Map docMapping = makeElasticSearchMapping((String) dd.dataDocumentId, eci)
            client.admin().indices().preparePutMapping(indexName).setType((String) dd.dataDocumentId)
                    .setSource(docMapping).execute().actionGet() // .setIgnoreConflicts(true) no longer supported?
        }
    }

    static final Map<String, String> esTypeMap = [id:'keyword', 'id-long':'keyword', date:'date', time:'text',
            'date-time':'date', 'number-integer':'long', 'number-decimal':'double', 'number-float':'double',
            'currency-amount':'double', 'currency-precise':'double', 'text-indicator':'keyword', 'text-short':'text',
            'text-medium':'text', 'text-long':'text', 'text-very-long':'text', 'binary-very-long':'binary']

    static Map makeElasticSearchMapping(String dataDocumentId, ExecutionContextImpl eci) {
        EntityValue dataDocument = eci.entityFacade.find("moqui.entity.document.DataDocument")
                .condition("dataDocumentId", dataDocumentId).useCache(true).one()
        if (dataDocument == null) throw new EntityException("No DataDocument found with ID [${dataDocumentId}]")
        EntityList dataDocumentFieldList = dataDocument.findRelated("moqui.entity.document.DataDocumentField", null, null, true, false)
        EntityList dataDocumentRelAliasList = dataDocument.findRelated("moqui.entity.document.DataDocumentRelAlias", null, null, true, false)

        Map<String, String> relationshipAliasMap = [:]
        for (EntityValue dataDocumentRelAlias in dataDocumentRelAliasList)
            relationshipAliasMap.put((String) dataDocumentRelAlias.relationshipName, (String) dataDocumentRelAlias.documentAlias)

        String primaryEntityName = dataDocument.primaryEntityName
        // String primaryEntityAlias = relationshipAliasMap.get(primaryEntityName) ?: primaryEntityName
        EntityDefinition primaryEd = eci.entityFacade.getEntityDefinition(primaryEntityName)

        Map<String, Object> rootProperties = [_entity:[type:'keyword']] as Map<String, Object>
        Map<String, Object> mappingMap = [properties:rootProperties] as Map<String, Object>

        List<String> remainingPkFields = new ArrayList(primaryEd.getPkFieldNames())
        for (EntityValue dataDocumentField in dataDocumentFieldList) {
            String fieldPath = dataDocumentField.fieldPath
            if (!fieldPath.contains(':')) {
                // is a field on the primary entity, put it there
                String fieldName = dataDocumentField.fieldNameAlias ?: dataDocumentField.fieldPath
                FieldInfo fieldInfo = primaryEd.getFieldInfo((String) dataDocumentField.fieldPath)
                if (fieldInfo == null) throw new EntityException("Could not find field [${dataDocumentField.fieldPath}] for entity [${primaryEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                rootProperties.put(fieldName, makePropertyMap(fieldInfo.type))

                if (remainingPkFields.contains(dataDocumentField.fieldPath)) remainingPkFields.remove((String) dataDocumentField.fieldPath)
                continue
            }

            Iterator<String> fieldPathElementIter = fieldPath.split(":").iterator()
            Map<String, Object> currentProperties = rootProperties
            EntityDefinition currentEd = primaryEd
            while (fieldPathElementIter.hasNext()) {
                String fieldPathElement = fieldPathElementIter.next()
                if (fieldPathElementIter.hasNext()) {
                    EntityJavaUtil.RelationshipInfo relInfo = currentEd.getRelationshipInfo(fieldPathElement)
                    if (relInfo == null) throw new EntityException("Could not find relationship [${fieldPathElement}] for entity [${currentEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                    currentEd = relInfo.relatedEd
                    if (currentEd == null) throw new EntityException("Could not find entity [${relInfo.relatedEntityName}] in DataDocument [${dataDocumentId}]")

                    // only put type many in sub-objects, same as DataDocument generation
                    if (!relInfo.isTypeOne) {
                        String objectName = relationshipAliasMap.get(fieldPathElement) ?: fieldPathElement
                        Map<String, Object> subObject = (Map<String, Object>) currentProperties.get(objectName)
                        Map<String, Object> subProperties
                        if (subObject == null) {
                            subProperties = new HashMap<>()
                            // NOTE: not doing type:'nested', ES docs say arrays should have it but they go into separate documents and term/facet searches fail!
                            subObject = [properties:subProperties] as Map<String, Object>
                            currentProperties.put(objectName, subObject)
                        } else {
                            subProperties = (Map<String, Object>) subObject.get("properties")
                        }
                        currentProperties = subProperties
                    }
                } else {
                    String fieldName = (String) dataDocumentField.fieldNameAlias ?: fieldPathElement
                    FieldInfo fieldInfo = currentEd.getFieldInfo(fieldPathElement)
                    if (fieldInfo == null) throw new EntityException("Could not find field [${fieldPathElement}] for entity [${currentEd.getFullEntityName()}] in DataDocument [${dataDocumentId}]")
                    currentProperties.put(fieldName, makePropertyMap(fieldInfo.type))

                    // logger.info("DataDocument ${dataDocumentId} field ${fieldName}, propertyMap: ${propertyMap}")
                }
            }
        }

        // now get all the PK fields not aliased explicitly
        for (String remainingPkName in remainingPkFields) {
            FieldInfo fieldInfo = primaryEd.getFieldInfo(remainingPkName)
            String fieldType = fieldInfo.type
            String mappingType = esTypeMap.get(fieldType) ?: 'keyword'
            Map propertyMap = [type:mappingType]
            // if (fieldType.startsWith("id")) propertyMap.index = 'not_analyzed'
            rootProperties.put(remainingPkName, propertyMap)
        }

        if (logger.isTraceEnabled()) logger.trace("Generated ElasticSearch mapping for ${dataDocumentId}: \n${JsonOutput.prettyPrint(JsonOutput.toJson(mappingMap))}")

        return mappingMap
    }
    static Map makePropertyMap(String fieldType) {
        String mappingType = esTypeMap.get(fieldType) ?: 'text'
        Map propertyMap = [type:mappingType]
        if ("date".equals(mappingType)) propertyMap.format = "strict_date_optional_time||epoch_millis||yyyy-MM-dd HH:mm:ss.SSS||yyyy-MM-dd HH:mm:ss.S||yyyy-MM-dd"
        // if (fieldType.startsWith("id")) propertyMap.index = 'not_analyzed'
        return propertyMap
    }
}
