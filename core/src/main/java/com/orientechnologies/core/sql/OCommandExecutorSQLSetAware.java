/*
 *
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.core.sql;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.core.config.YTGlobalConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 *
 */
public abstract class OCommandExecutorSQLSetAware extends OCommandExecutorSQLAbstract {

  protected static final String KEYWORD_SET = "SET";
  protected static final String KEYWORD_CONTENT = "CONTENT";

  protected YTEntityImpl content = null;
  protected int parameterCounter = 0;

  protected void parseContent() {
    if (!parserIsEnded() && !parserGetLastWord().equals(KEYWORD_WHERE)) {
      content = parseJSON();
    }

    if (content == null) {
      throwSyntaxErrorException("Content not provided. Example: CONTENT { \"name\": \"Jay\" }");
    }
  }

  protected void parseSetFields(final YTClass iClass, final List<OPair<String, Object>> fields) {
    String fieldName;
    String fieldValue;

    while (!parserIsEnded()
        && (fields.size() == 0
        || parserGetLastSeparator() == ','
        || parserGetCurrentChar() == ',')) {
      fieldName = parserRequiredWord(false, "Field name expected");
      if (fieldName.equalsIgnoreCase(KEYWORD_WHERE)) {
        parserGoBack();
        break;
      }

      parserNextChars(false, true, "=");
      fieldValue = parserRequiredWord(false, "Value expected", " =><,\r\n");

      // INSERT TRANSFORMED FIELD VALUE
      final Object v = convertValue(iClass, fieldName, getFieldValueCountingParameters(fieldValue));

      fields.add(new OPair(fieldName, v));
      parserSkipWhiteSpaces();
    }

    if (fields.size() == 0) {
      throwParsingException(
          "Entries to set <field> = <value> are missed. Example: name = 'Bill', salary = 300.2");
    }
  }

  protected YTClass extractClassFromTarget(String iTarget) {
    // CLASS
    if (!iTarget.toUpperCase(Locale.ENGLISH).startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)
        && !iTarget.startsWith(OCommandExecutorSQLAbstract.INDEX_PREFIX)) {

      if (iTarget.toUpperCase(Locale.ENGLISH).startsWith(OCommandExecutorSQLAbstract.CLASS_PREFIX))
      // REMOVE CLASS PREFIX
      {
        iTarget = iTarget.substring(OCommandExecutorSQLAbstract.CLASS_PREFIX.length());
      }

      if (iTarget.charAt(0) == YTRID.PREFIX) {
        return getDatabase()
            .getMetadata()
            .getImmutableSchemaSnapshot()
            .getClassByClusterId(new YTRecordId(iTarget).getClusterId());
      }

      return getDatabase().getMetadata().getImmutableSchemaSnapshot().getClass(iTarget);
    }
    // CLUSTER
    if (iTarget
        .toUpperCase(Locale.ENGLISH)
        .startsWith(OCommandExecutorSQLAbstract.CLUSTER_PREFIX)) {
      String clusterName =
          iTarget.substring(OCommandExecutorSQLAbstract.CLUSTER_PREFIX.length()).trim();
      YTDatabaseSessionInternal db = getDatabase();
      if (clusterName.startsWith("[") && clusterName.endsWith("]")) {
        String[] clusterNames = clusterName.substring(1, clusterName.length() - 1).split(",");
        YTClass candidateClass = null;
        for (String cName : clusterNames) {
          final int clusterId = db.getClusterIdByName(cName.trim());
          if (clusterId < 0) {
            return null;
          }
          YTClass aClass =
              db.getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(clusterId);
          if (aClass == null) {
            return null;
          }
          if (candidateClass == null
              || candidateClass.equals(aClass)
              || candidateClass.isSubClassOf(aClass)) {
            candidateClass = aClass;
          } else if (!candidateClass.isSuperClassOf(aClass)) {
            return null;
          }
        }
        return candidateClass;
      } else {
        final int clusterId = db.getClusterIdByName(clusterName);
        if (clusterId >= 0) {
          return db.getMetadata().getImmutableSchemaSnapshot().getClassByClusterId(clusterId);
        }
      }
    }
    return null;
  }

  protected Object convertValue(YTClass iClass, String fieldName, Object v) {
    if (iClass != null) {
      // CHECK TYPE AND CONVERT IF NEEDED
      final YTProperty p = iClass.getProperty(fieldName);
      if (p != null) {
        final YTClass embeddedType = p.getLinkedClass();

        switch (p.getType()) {
          case EMBEDDED:
            // CONVERT MAP IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
            if (v instanceof Map) {
              v = createDocumentFromMap(embeddedType, (Map<String, Object>) v);
            }
            break;

          case EMBEDDEDSET:
            // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
            if (v instanceof Map) {
              return createDocumentFromMap(embeddedType, (Map<String, Object>) v);
            } else if (OMultiValue.isMultiValue(v)) {
              final Set set = new HashSet();

              for (Object o : OMultiValue.getMultiValueIterable(v)) {
                if (o instanceof Map) {
                  final YTEntityImpl doc =
                      createDocumentFromMap(embeddedType, (Map<String, Object>) o);
                  set.add(doc);
                } else if (o instanceof YTIdentifiable) {
                  set.add(((YTIdentifiable) o).getRecord());
                } else {
                  set.add(o);
                }
              }

              v = set;
            }
            break;

          case EMBEDDEDLIST:
            // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
            if (v instanceof Map) {
              return createDocumentFromMap(embeddedType, (Map<String, Object>) v);
            } else if (OMultiValue.isMultiValue(v)) {
              final List set = new ArrayList();

              for (Object o : OMultiValue.getMultiValueIterable(v)) {
                if (o instanceof Map) {
                  final YTEntityImpl doc =
                      createDocumentFromMap(embeddedType, (Map<String, Object>) o);
                  set.add(doc);
                } else if (o instanceof YTIdentifiable) {
                  set.add(((YTIdentifiable) o).getRecord());
                } else {
                  set.add(o);
                }
              }

              v = set;
            }
            break;

          case EMBEDDEDMAP:
            // CONVERT MAPS IN DOCUMENTS ASSIGNING THE CLASS TAKEN FROM SCHEMA
            if (v instanceof Map) {
              final Map<String, Object> map = new HashMap<String, Object>();

              for (Map.Entry<String, Object> entry : ((Map<String, Object>) v).entrySet()) {
                if (entry.getValue() instanceof Map) {
                  final YTEntityImpl doc =
                      createDocumentFromMap(embeddedType, (Map<String, Object>) entry.getValue());
                  map.put(entry.getKey(), doc);
                } else if (entry.getValue() instanceof YTIdentifiable) {
                  map.put(entry.getKey(), ((YTIdentifiable) entry.getValue()).getRecord());
                } else {
                  map.put(entry.getKey(), entry.getValue());
                }
              }

              v = map;
            }
            break;
        }
      }
    }
    return v;
  }

  private YTEntityImpl createDocumentFromMap(YTClass embeddedType, Map<String, Object> o) {
    final YTEntityImpl doc = new YTEntityImpl();
    if (embeddedType != null) {
      doc.setClassName(embeddedType.getName());
    }

    doc.fromMap(o);
    return doc;
  }

  @Override
  public long getDistributedTimeout() {
    return getDatabase()
        .getConfiguration()
        .getValueAsLong(YTGlobalConfiguration.DISTRIBUTED_COMMAND_TASK_SYNCH_TIMEOUT);
  }

  protected Object getFieldValueCountingParameters(String fieldValue) {
    if (fieldValue.trim().equals("?")) {
      parameterCounter++;
    }
    return OSQLHelper.parseValue(this, fieldValue, context, true);
  }

  protected YTEntityImpl parseJSON() {
    final String contentAsString = parserRequiredWord(false, "JSON expected").trim();
    final YTEntityImpl json = new YTEntityImpl();
    json.fromJSON(contentAsString);
    parserSkipWhiteSpaces();
    return json;
  }
}
