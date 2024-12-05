/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.core.serialization.serializer.record.string;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.parser.OStringParser;
import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.LinkList;
import com.orientechnologies.core.db.record.LinkSet;
import com.orientechnologies.core.db.record.TrackedList;
import com.orientechnologies.core.db.record.TrackedSet;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.fetch.OFetchHelper;
import com.orientechnologies.core.fetch.OFetchPlan;
import com.orientechnologies.core.fetch.json.OJSONFetchContext;
import com.orientechnologies.core.fetch.json.OJSONFetchListener;
import com.orientechnologies.core.id.ChangeableRecordId;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.ORecordStringable;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.impl.ODocumentHelper;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTBlob;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTEntityImplEmbedded;
import com.orientechnologies.core.serialization.serializer.OJSONWriter;
import com.orientechnologies.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.core.util.ODateHelper;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.StringWriter;
import java.text.ParseException;
import java.util.Base64;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class ORecordSerializerJSON extends ORecordSerializerStringAbstract {

  public static final String NAME = "json";
  public static final ORecordSerializerJSON INSTANCE = new ORecordSerializerJSON();
  public static final char[] PARAMETER_SEPARATOR = new char[]{':', ','};
  public static final int INITIAL_SIZE = 5000;
  private static final Long MAX_INT = (long) Integer.MAX_VALUE;
  private static final Long MIN_INT = (long) Integer.MIN_VALUE;

  private interface CollectionItemVisitor {

    void visitItem(Object item);
  }

  public static class FormatSettings {

    public boolean includeVer;
    public boolean includeType;
    public boolean includeId;
    public boolean includeClazz;
    public boolean attribSameRow;
    public boolean alwaysFetchEmbeddedDocuments;
    public int indentLevel;
    public String fetchPlan = null;
    public boolean keepTypes = true;
    public boolean dateAsLong = false;
    public boolean prettyPrint = false;

    public FormatSettings(final String iFormat) {
      if (iFormat == null) {
        includeType = true;
        includeVer = true;
        includeId = true;
        includeClazz = true;
        attribSameRow = true;
        indentLevel = 0;
        fetchPlan = "";
        alwaysFetchEmbeddedDocuments = true;
      } else {
        includeType = false;
        includeVer = false;
        includeId = false;
        includeClazz = false;
        attribSameRow = false;
        alwaysFetchEmbeddedDocuments = false;
        indentLevel = 0;
        keepTypes = false;

        if (!iFormat.isEmpty()) {
          final String[] format = iFormat.split(",");
          for (String f : format) {
            if (f.equals("type")) {
              includeType = true;
            } else {
              if (f.equals("rid")) {
                includeId = true;
              } else {
                if (f.equals("version")) {
                  includeVer = true;
                } else {
                  if (f.equals("class")) {
                    includeClazz = true;
                  } else {
                    if (f.equals("attribSameRow")) {
                      attribSameRow = true;
                    } else {
                      if (f.startsWith("indent")) {
                        indentLevel = Integer.parseInt(f.substring(f.indexOf(':') + 1));
                      } else {
                        if (f.startsWith("fetchPlan")) {
                          fetchPlan = f.substring(f.indexOf(':') + 1);
                        } else {
                          if (f.startsWith("keepTypes")) {
                            keepTypes = true;
                          } else {
                            if (f.startsWith("alwaysFetchEmbedded")) {
                              alwaysFetchEmbeddedDocuments = true;
                            } else {
                              if (f.startsWith("dateAsLong")) {
                                dateAsLong = true;
                              } else {
                                if (f.startsWith("prettyPrint")) {
                                  prettyPrint = true;
                                } else {
                                  if (f.startsWith("graph") || f.startsWith("shallow"))
                                    // SUPPORTED IN OTHER PARTS
                                    ;
                                  else {
                                    throw new IllegalArgumentException(
                                        "Unrecognized JSON formatting option: " + f);
                                  }
                                }
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  public YTRecordAbstract fromString(
      YTDatabaseSessionInternal db, String source, YTRecordAbstract record, final String[] fields,
      boolean needReload) {
    return fromString(db, source, record, fields, null, needReload);
  }

  @Override
  public YTRecordAbstract fromString(YTDatabaseSessionInternal db, String source,
      YTRecordAbstract record, final String[] fields) {
    return fromString(db, source, record, fields, null, false);
  }

  public YTRecordAbstract fromString(
      YTDatabaseSessionInternal db, String source,
      YTRecordAbstract record,
      final String[] fields,
      final String options,
      boolean needReload) {
    return fromString(db, source, record, fields, options, needReload, -1, new IntOpenHashSet());
  }

  public YTRecordAbstract fromString(
      YTDatabaseSessionInternal db, String source,
      YTRecordAbstract record,
      final String[] iFields,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      IntSet skippedPartsIndexes) {
    return this.fromStringV0(db,
        source, record, iOptions, needReload, maxRidbagSizeBeforeSkip, skippedPartsIndexes);
  }

  public YTRecordAbstract fromStringV0(
      YTDatabaseSessionInternal db, String source,
      YTRecordAbstract record,
      final String iOptions,
      boolean needReload,
      int maxRidbagSizeBeforeSkip,
      IntSet skippedPartsIndexes) {
    source = source.trim();
    boolean brackets = source.startsWith("{") && source.endsWith("}");

    String className = null;
    boolean noMap = false;
    if (iOptions != null) {
      final String[] format = iOptions.split(",");
      for (String f : format) {
        if (f.equalsIgnoreCase("noMap")) {
          noMap = true;
          break;
        }
      }
    }

    if (record != null)
    // RESET ALL THE FIELDS
    {
      record.clear();
    }

    final List<String> fields =
        OStringSerializerHelper.smartSplit(
            source,
            PARAMETER_SEPARATOR,
            brackets ? 1 : 0,
            brackets ? (source.length() - 2) : -1,
            true,
            true,
            false,
            false,
            maxRidbagSizeBeforeSkip,
            skippedPartsIndexes,
            ' ',
            '\n',
            '\r',
            '\t');

    if (fields.size() % 2 != 0) {
      throw new YTSerializationException(
          "Error on unmarshalling JSON content: wrong format \""
              + source
              + "\". Use <field> : <value>");
    }

    Map<String, Character> fieldTypes = null;

    if (!fields.isEmpty()) {
      // SEARCH FOR FIELD TYPES IF ANY
      for (int i = 0; i < fields.size(); i += 2) {
        final String fieldName = OIOUtils.getStringContent(fields.get(i));
        final String fieldValue = fields.get(i + 1);
        final String fieldValueAsString = OIOUtils.getStringContent(fieldValue);

        if (fieldName.equals(OFieldTypesString.ATTRIBUTE_FIELD_TYPES)
            && record instanceof YTEntityImpl) {
          fieldTypes = OFieldTypesString.loadFieldTypesV0(fieldTypes, fieldValueAsString);
        } else {
          if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
            if (record == null
                || ORecordInternal.getRecordType(record) != fieldValueAsString.charAt(0)) {
              // CREATE THE RIGHT RECORD INSTANCE
              record =
                  YouTrackDBManager.instance()
                      .getRecordFactoryManager()
                      .newInstance(
                          (byte) fieldValueAsString.charAt(0),
                          new ChangeableRecordId(),
                          ODatabaseRecordThreadLocal.instance().getIfDefined());
            }
          } else {
            if (needReload
                && fieldName.equals(ODocumentHelper.ATTRIBUTE_RID)
                && record instanceof YTEntityImpl) {
              if (fieldValue != null && !fieldValue.isEmpty()) {
                try {
                  record =
                      ODatabaseRecordThreadLocal.instance()
                          .get()
                          .load(new YTRecordId(fieldValueAsString));

                } catch (YTRecordNotFoundException e) {
                  // ignore
                }
              }
            } else {
              if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS)
                  && record instanceof YTEntityImpl) {
                className = "null".equals(fieldValueAsString) ? null : fieldValueAsString;
                ODocumentInternal.fillClassNameIfNeeded(((YTEntityImpl) record), className);
              }
            }
          }
        }
      }

      if (record == null) {
        record = new YTEntityImpl();
        ORecordInternal.unsetDirty(record);
      }

      try {
        for (int i = 0; i < fields.size(); i += 2) {
          final String fieldName = OIOUtils.getStringContent(fields.get(i));
          final String fieldValue = fields.get(i + 1);
          final String fieldValueAsString = OIOUtils.getStringContent(fieldValue);

          processRecordsV0(db,
              record, fieldTypes, noMap, iOptions, fieldName, fieldValue, fieldValueAsString);
        }
        if (className != null) {
          // Trigger the default value
          ((YTEntityImpl) record).setClassName(className);
        }
      } catch (Exception e) {
        if (record.getIdentity().isValid()) {
          throw YTException.wrapException(
              new YTSerializationException(
                  "Error on unmarshalling JSON content for record " + record.getIdentity()),
              e);
        } else {
          throw YTException.wrapException(
              new YTSerializationException(
                  "Error on unmarshalling JSON content for record: " + source),
              e);
        }
      }
    }
    return record;
  }

  private void processRecordsV0(
      YTDatabaseSessionInternal db, YTRecordAbstract record,
      Map<String, Character> fieldTypes,
      boolean noMap,
      String iOptions,
      String fieldName,
      String fieldValue,
      String fieldValueAsString) {
    // RECORD ATTRIBUTES
    if (fieldName.equals(ODocumentHelper.ATTRIBUTE_RID)) {
      ORecordInternal.setIdentity(record, new YTRecordId(fieldValueAsString));
    } else {
      if (fieldName.equals(ODocumentHelper.ATTRIBUTE_VERSION)) {
        ORecordInternal.setVersion(record, Integer.parseInt(fieldValue));
      } else {
        if (fieldName.equals(ODocumentHelper.ATTRIBUTE_CLASS)) {
        } else {
          if (fieldName.equals(ODocumentHelper.ATTRIBUTE_TYPE)) {
          } else {
            if (fieldName.equals(OFieldTypesString.ATTRIBUTE_FIELD_TYPES)
                && record instanceof YTEntityImpl) {
            } else {
              if (fieldName.equals("value") && !(record instanceof YTEntityImpl)) {
                // RECORD VALUE(S)
                if ("null".equals(fieldValue)) {
                  record.fromStream(OCommonConst.EMPTY_BYTE_ARRAY);
                } else {
                  if (record instanceof YTBlob) {
                    // BYTES
                    ORecordInternal.unsetDirty(record);
                    ORecordInternal.fill(
                        record,
                        record.getIdentity(),
                        record.getVersion(),
                        Base64.getDecoder().decode(fieldValueAsString),
                        true);
                  } else {
                    if (record instanceof ORecordStringable) {
                      ((ORecordStringable) record).value(fieldValueAsString);
                    } else {
                      throw new IllegalArgumentException("unsupported type of record");
                    }
                  }
                }
              } else {
                if (record instanceof YTEntityImpl doc) {

                  // DETERMINE THE TYPE FROM THE SCHEMA
                  YTType type = determineType(doc, fieldName);

                  final Object v;
                  if (OStringSerializerHelper.SKIPPED_VALUE.equals(fieldValue)) {
                    v = new RidBag(db);
                  } else {
                    v =
                        getValueV0(db,
                            doc,
                            fieldName,
                            fieldValue,
                            fieldValueAsString,
                            type,
                            null,
                            fieldTypes,
                            noMap, iOptions);
                  }

                  if (v != null) {
                    if (v instanceof Collection<?> && !((Collection<?>) v).isEmpty()) {
                      // CHECK IF THE COLLECTION IS EMBEDDED
                      if (type == null) {
                        // TRY TO UNDERSTAND BY FIRST ITEM
                        Object first = ((Collection<?>) v).iterator().next();
                        if (first instanceof YTRecord
                            && !((YTRecord) first).getIdentity().isValid()) {
                          type = v instanceof Set<?> ? YTType.EMBEDDEDSET : YTType.EMBEDDEDLIST;
                        }
                      }

                      if (type != null) {
                        // TREAT IT AS EMBEDDED
                        doc.setPropertyInternal(fieldName, v, type);
                        return;
                      }
                    } else {
                      if (v instanceof Map<?, ?> && !((Map<?, ?>) v).isEmpty()) {
                        // CHECK IF THE MAP IS EMBEDDED
                        Object first = ((Map<?, ?>) v).values().iterator().next();
                        if (first instanceof YTRecord
                            && !((YTRecord) first).getIdentity().isValid()) {
                          doc.setProperty(fieldName, v, YTType.EMBEDDEDMAP);
                          return;
                        }
                      } else {
                        if (v instanceof YTEntityImpl && type != null && type.isLink()) {
                          String className1 = ((YTEntityImpl) v).getClassName();
                          if (className1 != null && !className1.isEmpty()) {
                            ((YTEntityImpl) v).save();
                          }
                        }
                      }
                    }
                  }

                  if (type == null && fieldTypes != null && fieldTypes.containsKey(fieldName)) {
                    type = OFieldTypesString.getType(fieldValue, fieldTypes.get(fieldName));
                  }

                  if (v instanceof TrackedSet<?>) {
                    if (v instanceof LinkSet || OMultiValue.getFirstValue(
                        v) instanceof YTIdentifiable) {
                      type = YTType.LINKSET;
                    }
                  } else {
                    if (v instanceof TrackedList<?>) {
                      if (OMultiValue.getFirstValue(v) instanceof YTIdentifiable) {
                        type = YTType.LINKLIST;
                      }
                    }
                  }

                  if (type != null) {
                    doc.setPropertyInternal(fieldName, v, type);
                  } else {
                    doc.setPropertyInternal(fieldName, v);
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  public void toString(final YTRecord iRecord, final OJSONWriter json, final String iFormat) {
    try {
      final FormatSettings settings = new FormatSettings(iFormat);

      json.beginObject();

      OJSONFetchContext context = new OJSONFetchContext(json, settings);
      context.writeSignature(json, iRecord);

      if (iRecord instanceof YTEntityImpl) {
        final OFetchPlan fp = OFetchHelper.buildFetchPlan(settings.fetchPlan);

        OFetchHelper.fetch(iRecord, null, fp, new OJSONFetchListener(), context, iFormat);
      } else {
        if (iRecord instanceof ORecordStringable record) {

          // STRINGABLE
          json.writeAttribute(settings.indentLevel, true, "value", record.value());

        } else {
          if (iRecord instanceof YTBlob record) {
            // BYTES
            json.writeAttribute(
                settings.indentLevel,
                true,
                "value",
                Base64.getEncoder().encodeToString(((YTRecordAbstract) record).toStream()));
          } else {
            throw new YTSerializationException(
                "Error on marshalling record of type '"
                    + iRecord.getClass()
                    + "' to JSON. The record type cannot be exported to JSON");
          }
        }
      }

      json.endObject(settings.indentLevel, true);
    } catch (IOException e) {
      throw YTException.wrapException(
          new YTSerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public StringBuilder toString(
      final YTRecord record,
      final StringBuilder output,
      final String format,
      boolean autoDetectCollectionType) {
    try {
      final StringWriter buffer = new StringWriter(INITIAL_SIZE);
      final OJSONWriter json = new OJSONWriter(buffer, format);
      final FormatSettings settings = new FormatSettings(format);

      json.beginObject();

      final OJSONFetchContext context = new OJSONFetchContext(json, settings);
      context.writeSignature(json, record);

      if (record instanceof YTEntityImpl) {
        final OFetchPlan fp = OFetchHelper.buildFetchPlan(settings.fetchPlan);

        OFetchHelper.fetch(record, null, fp, new OJSONFetchListener(), context, format);
      } else {
        if (record instanceof ORecordStringable recordStringable) {
          // STRINGABLE
          json.writeAttribute(settings.indentLevel, true, "value", recordStringable.value());
        } else {
          if (record instanceof YTBlob recordBlob) {
            // BYTES
            json.writeAttribute(
                settings.indentLevel,
                true,
                "value",
                Base64.getEncoder().encodeToString(((YTRecordAbstract) recordBlob).toStream()));
          } else {
            throw new YTSerializationException(
                "Error on marshalling record of type '"
                    + record.getClass()
                    + "' to JSON. The record type cannot be exported to JSON");
          }
        }
      }
      json.endObject(settings.indentLevel, true);

      output.append(buffer);
      return output;
    } catch (final IOException e) {
      throw YTException.wrapException(
          new YTSerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  private YTType determineType(YTEntityImpl doc, String fieldName) {
    YTType type = null;
    final YTClass cls = ODocumentInternal.getImmutableSchemaClass(doc);
    if (cls != null) {
      final YTProperty prop = cls.getProperty(fieldName);
      if (prop != null) {
        type = prop.getType();
      }
    }
    return type;
  }

  private Object getValueV0(
      YTDatabaseSessionInternal db, final YTEntityImpl iRecord,
      String iFieldName,
      String iFieldValue,
      String iFieldValueAsString,
      YTType iType,
      YTType iLinkedType,
      final Map<String, Character> iFieldTypes,
      final boolean iNoMap,
      final String iOptions) {
    if (iFieldValue.equals("null")) {
      return null;
    }

    if (iFieldName != null && ODocumentInternal.getImmutableSchemaClass(iRecord) != null) {
      final YTProperty p =
          ODocumentInternal.getImmutableSchemaClass(iRecord).getProperty(iFieldName);
      if (p != null) {
        iType = p.getType();
        iLinkedType = p.getLinkedType();
      }
    }

    if (iType == null && iFieldTypes != null && iFieldTypes.containsKey(iFieldName)) {
      iType = OFieldTypesString.getType(iFieldValue, iFieldTypes.get(iFieldName));
    }

    if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
      // Json object
      return getValueAsObjectOrMapV0(db,
          iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
    } else {
      if (iFieldValue.startsWith("[") && iFieldValue.endsWith("]")) {
        // Json array
        return getValueAsCollectionV0(db,
            iRecord, iFieldValue, iType, iLinkedType, iFieldTypes, iNoMap, iOptions);
      }
    }

    if (iType == null || iType == YTType.ANY) {
      // TRY TO DETERMINE THE CONTAINED TYPE from THE FIRST VALUE
      if (iFieldValue.charAt(0) != '\"' && iFieldValue.charAt(0) != '\'') {
        if (iFieldValue.equalsIgnoreCase("false") || iFieldValue.equalsIgnoreCase("true")) {
          iType = YTType.BOOLEAN;
        } else {
          Character c = null;
          if (iFieldTypes != null) {
            c = iFieldTypes.get(iFieldName);
            if (c != null) {
              iType = ORecordSerializerStringAbstract.getType(iFieldValue + c);
            }
          }

          if (c == null) {
            // TRY TO AUTODETERMINE THE BEST TYPE
            if (YTRecordId.isA(iFieldValue)) {
              iType = YTType.LINK;
            } else {
              if (iFieldValue.matches(".*[.Ee].*")) {
                // DECIMAL FORMAT: DETERMINE IF DOUBLE OR FLOAT
                return Double.valueOf(OIOUtils.getStringContent(iFieldValue));
                // REMOVED TRUNK to float
                // if (canBeTrunkedToFloat(v))
                // return v.floatValue();
                // else
                // return v;
              } else {
                final Long v = Long.valueOf(OIOUtils.getStringContent(iFieldValue));
                // INTEGER FORMAT: DETERMINE IF DOUBLE OR FLOAT

                if (canBeTrunkedToInt(v)) {
                  return v.intValue();
                } else {
                  return v;
                }
              }
            }
          }
        }
      } else {
        if (iFieldValue.startsWith("{") && iFieldValue.endsWith("}")) {
          iType = YTType.EMBEDDED;
        } else {
          if (YTRecordId.isA(iFieldValueAsString)) {
            iType = YTType.LINK;
          }

          if (iFieldTypes != null) {
            Character c = iFieldTypes.get(iFieldName);
            if (c != null) {
              iType = OFieldTypesString.getType(iFieldValueAsString, c);
            }
          }

          if (iType == null) {
            iType = YTType.STRING;
          }
        }
      }
    }

    if (iType != null) {
      switch (iType) {
        case STRING:
          return decodeJSON(iFieldValueAsString);
        case LINK:
          final int pos = iFieldValueAsString.indexOf('@');
          if (pos > -1)
          // CREATE DOCUMENT
          {
            return new YTEntityImpl(
                iFieldValueAsString.substring(1, pos),
                new YTRecordId(iFieldValueAsString.substring(pos + 1)));
          } else {
            // CREATE SIMPLE RID
            return new YTRecordId(iFieldValueAsString);
          }
        case EMBEDDED:
          return fromString(db, iFieldValueAsString, new YTEntityImplEmbedded(), null);
        case DATE:
          if (iFieldValueAsString == null || iFieldValueAsString.isEmpty()) {
            return null;
          }
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(iFieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATE
              return ODateHelper.getDateFormatInstance().parseObject(iFieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw YTException.wrapException(
                  new YTSerializationException(
                      "Unable to unmarshall date (format="
                          + ODateHelper.getDateFormat()
                          + ") : "
                          + iFieldValueAsString),
                  ex);
            }
          }
        case DATETIME:
          if (iFieldValueAsString == null || iFieldValueAsString.isEmpty()) {
            return null;
          }
          try {
            // TRY TO PARSE AS LONG
            return Long.parseLong(iFieldValueAsString);
          } catch (NumberFormatException e) {
            try {
              // TRY TO PARSE AS DATETIME
              return ODateHelper.getDateTimeFormatInstance().parseObject(iFieldValueAsString);
            } catch (ParseException ex) {
              OLogManager.instance()
                  .error(this, "Exception is suppressed, original exception is ", e);
              throw YTException.wrapException(
                  new YTSerializationException(
                      "Unable to unmarshall datetime (format="
                          + ODateHelper.getDateTimeFormat()
                          + ") : "
                          + iFieldValueAsString),
                  ex);
            }
          }
        case BINARY:
          return OStringSerializerHelper.fieldTypeFromStream(db, iRecord, iType,
              iFieldValueAsString);
        case CUSTOM: {
          try {
            ByteArrayInputStream bais =
                new ByteArrayInputStream(Base64.getDecoder().decode(iFieldValueAsString));
            ObjectInputStream input = new ObjectInputStream(bais);
            return input.readObject();
          } catch (IOException | ClassNotFoundException e) {
            throw YTException.wrapException(
                new YTSerializationException("Error on custom field deserialization"), e);
          }
        }
        default:
          return OStringSerializerHelper.fieldTypeFromStream(db, iRecord, iType, iFieldValue);
      }
    }
    return iFieldValueAsString;
  }

  private boolean canBeTrunkedToInt(Long v) {
    return (v > 0) ? v.compareTo(MAX_INT) <= 0 : v.compareTo(MIN_INT) >= 0;
  }

  /**
   * OBJECT OR MAP. CHECK THE TYPE ATTRIBUTE TO KNOW IT.
   */
  private Object getValueAsObjectOrMapV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {
    final String[] fields =
        OStringParser.getWords(iFieldValue.substring(1, iFieldValue.length() - 1), ":,", true);

    if (fields.length == 0) {
      if (iNoMap) {
        YTEntityImpl res = new YTEntityImpl();
        ODocumentInternal.addOwner(res, iRecord);
        return res;
      } else {
        return new HashMap<String, Object>();
      }
    }

    if (iNoMap || hasTypeField(fields)) {
      return getObjectValuesAsRecordV0(db, iRecord, iFieldValue, iType, iOptions, fields);
    } else {
      return getObjectValuesAsMapV0(db,
          iRecord, iFieldValue, iLinkedType, iFieldTypes, iOptions, fields);
    }
  }

  private Object getObjectValuesAsMapV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord,
      String iFieldValue,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      String iOptions,
      String[] fields) {
    if (fields.length % 2 == 1) {
      throw new YTSerializationException(
          "Bad JSON format on map. Expected pairs of field:value but received '"
              + iFieldValue
              + "'");
    }
    final Map<String, Object> embeddedMap = new LinkedHashMap<>();

    for (int i = 0; i < fields.length; i += 2) {
      String iFieldName = fields[i];
      if (iFieldName.length() >= 2) {
        iFieldName = iFieldName.substring(1, iFieldName.length() - 1);
      }
      iFieldValue = fields[i + 1];
      final String valueAsString = OIOUtils.getStringContent(iFieldValue);

      embeddedMap.put(
          iFieldName,
          getValueV0(db,
              iRecord,
              null,
              iFieldValue,
              valueAsString,
              iLinkedType,
              null,
              iFieldTypes,
              false, iOptions));
    }
    return embeddedMap;
  }

  private Object getObjectValuesAsRecordV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord, String iFieldValue, YTType iType,
      String iOptions, String[] fields) {
    YTRID rid = new YTRecordId(OIOUtils.getStringContent(getFieldValue("@rid", fields)));
    boolean shouldReload = rid.isTemporary();

    final YTEntityImpl recordInternal =
        (YTEntityImpl) fromString(db, iFieldValue, new YTEntityImpl(), null, iOptions,
            shouldReload);

    if (shouldBeDeserializedAsEmbedded(recordInternal, iType)) {
      ODocumentInternal.addOwner(recordInternal, iRecord);
    } else {
      YTDatabaseSession database = ODatabaseRecordThreadLocal.instance().getIfDefined();

      if (rid.isPersistent() && database != null) {
        YTEntityImpl documentToMerge = database.load(rid);
        documentToMerge.merge(recordInternal, false, false);
        return documentToMerge;
      }
    }
    return recordInternal;
  }

  private Object getValueAsCollectionV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {
    // remove square brackets
    iFieldValue = iFieldValue.substring(1, iFieldValue.length() - 1);

    if (iType == YTType.LINKBAG) {
      final RidBag bag = new RidBag(db);

      parseRidBagV0(db,
          iRecord,
          iFieldValue,
          iType,
          iFieldTypes,
          iNoMap,
          iOptions, new CollectionItemVisitor() {
            @Override
            public void visitItem(Object item) {
              bag.add((YTIdentifiable) item);
            }
          });

      return bag;
    } else {
      if (iType == YTType.LINKSET) {
        return getValueAsLinkedCollectionV0(db,
            new LinkSet(iRecord),
            iRecord,
            iFieldValue,
            iType,
            iLinkedType,
            iFieldTypes,
            iNoMap, iOptions);
      } else {
        if (iType == YTType.LINKLIST) {
          return getValueAsLinkedCollectionV0(db,
              new LinkList(iRecord),
              iRecord,
              iFieldValue,
              iType,
              iLinkedType,
              iFieldTypes,
              iNoMap, iOptions);
        } else {
          if (iType == YTType.EMBEDDEDSET) {
            return getValueAsEmbeddedCollectionV0(db,
                new TrackedSet<>(iRecord),
                iRecord,
                iFieldValue,
                iType,
                iLinkedType,
                iFieldTypes,
                iNoMap, iOptions);
          } else {
            return getValueAsEmbeddedCollectionV0(db,
                new TrackedList<>(iRecord),
                iRecord,
                iFieldValue,
                iType,
                iLinkedType,
                iFieldTypes,
                iNoMap, iOptions);
          }
        }
      }
    }
  }

  private Object getValueAsLinkedCollectionV0(
      YTDatabaseSessionInternal db, final Collection<YTIdentifiable> collection,
      YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {

    parseCollectionV0(db,
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions, new CollectionItemVisitor() {
          @Override
          public void visitItem(Object item) {
            collection.add((YTIdentifiable) item);
          }
        });

    return collection;
  }

  private Object getValueAsEmbeddedCollectionV0(
      YTDatabaseSessionInternal db, final Collection<Object> collection,
      YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions) {

    parseCollectionV0(db,
        iRecord,
        iFieldValue,
        iType,
        iLinkedType,
        iFieldTypes,
        iNoMap,
        iOptions, new CollectionItemVisitor() {
          @Override
          public void visitItem(Object item) {
            collection.add(item);
          }
        });
    return collection;
  }

  private void parseRidBagV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions,
      CollectionItemVisitor visitor) {
    if (!iFieldValue.isEmpty()) {
      int lastCommaPosition = -1;
      for (int i = 1; i < iFieldValue.length(); i++) {
        if (iFieldValue.charAt(i) == ',' || i == iFieldValue.length() - 1) {
          String item;
          if (i == iFieldValue.length() - 1) {
            item = iFieldValue.substring(lastCommaPosition + 1);
          } else {
            item = iFieldValue.substring(lastCommaPosition + 1, i);
          }
          lastCommaPosition = i;
          final String itemValue = item.trim();
          if (itemValue.isEmpty()) {
            continue;
          }

          final Object collectionItem =
              getValueV0(db,
                  iRecord,
                  null,
                  itemValue,
                  OIOUtils.getStringContent(itemValue),
                  YTType.LINK,
                  null,
                  iFieldTypes,
                  iNoMap, iOptions);

          // TODO: redundant in some cases, owner is already added by getValueV0 in some cases
          if (shouldBeDeserializedAsEmbedded(collectionItem, iType)) {
            ODocumentInternal.addOwner((YTEntityImpl) collectionItem, iRecord);
          }
          if (collectionItem != null) {
            visitor.visitItem(collectionItem);
          }
        }
      }
    }
  }

  private void parseCollectionV0(
      YTDatabaseSessionInternal db, YTEntityImpl iRecord,
      String iFieldValue,
      YTType iType,
      YTType iLinkedType,
      Map<String, Character> iFieldTypes,
      boolean iNoMap,
      String iOptions,
      CollectionItemVisitor visitor) {
    if (!iFieldValue.isEmpty()) {
      for (String item : OStringSerializerHelper.smartSplit(iFieldValue, ',')) {
        final String itemValue = item.trim();
        if (itemValue.isEmpty()) {
          continue;
        }

        final Object collectionItem =
            getValueV0(db,
                iRecord,
                null,
                itemValue,
                OIOUtils.getStringContent(itemValue),
                iLinkedType,
                null,
                iFieldTypes,
                iNoMap, iOptions);

        // TODO redundant in some cases, owner is already added by getValueV0 in some cases
        if (shouldBeDeserializedAsEmbedded(collectionItem, iType)) {
          ODocumentInternal.addOwner((YTEntityImpl) collectionItem, iRecord);
        }

        visitor.visitItem(collectionItem);
      }
    }
  }

  private boolean shouldBeDeserializedAsEmbedded(Object record, YTType iType) {
    return record instanceof YTEntityImpl
        && !((YTEntityImpl) record).getIdentity().isTemporary()
        && !((YTEntityImpl) record).getIdentity().isPersistent()
        && (iType == null || !iType.isLink());
  }

  private String decodeJSON(String iFieldValueAsString) {
    if (iFieldValueAsString == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder(iFieldValueAsString.length());
    boolean quoting = false;
    for (char c : iFieldValueAsString.toCharArray()) {
      if (quoting) {
        if (c != '\\' && c != '\"' && c != '/') {
          builder.append('\\');
        }
        builder.append(c);
        quoting = false;
      } else {
        if (c == '\\') {
          quoting = true;
        } else {
          builder.append(c);
        }
      }
    }
    return builder.toString();
  }

  private boolean hasTypeField(final String[] fields) {
    return hasField(fields);
  }

  /**
   * Checks if given collection of fields contain field with specified name.
   *
   * @param fields collection of fields where search
   * @return true if collection contain specified field, false otherwise.
   */
  private boolean hasField(final String[] fields) {
    return getFieldValue("@type", fields) != null;
  }

  private String getFieldValue(final String field, final String[] fields) {
    String doubleQuotes = "\"" + field + "\"";
    String singleQuotes = "'" + field + "'";
    for (int i = 0; i < fields.length; i = i + 2) {
      if (fields[i].equals(doubleQuotes) || fields[i].equals(singleQuotes)) {
        return fields[i + 1];
      }
    }
    return null;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
