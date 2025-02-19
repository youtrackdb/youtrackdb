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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.common.collection.MultiCollectionIterator;
import com.jetbrains.youtrack.db.internal.common.collection.MultiValue;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaImmutableClass;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class RecordSerializerSchemaAware2CSV extends RecordSerializerCSVAbstract {

  public static final String NAME = "ORecordDocument2csv";
  public static final RecordSerializerSchemaAware2CSV INSTANCE =
      new RecordSerializerSchemaAware2CSV();
  private static final long serialVersionUID = 1L;

  @Override
  public String toString() {
    return NAME;
  }

  public String getClassName(String content) {
    content = content.trim();

    if (content.length() == 0) {
      return null;
    }

    final var posFirstValue = content.indexOf(StringSerializerHelper.ENTRY_SEPARATOR);
    final var pos = content.indexOf(StringSerializerHelper.CLASS_SEPARATOR);

    if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) {
      return content.substring(0, pos);
    }

    return null;
  }

  @Override
  public RecordAbstract fromString(
      DatabaseSessionInternal session, String iContent, final RecordAbstract iRecord,
      final String[] iFields) {
    iContent = iContent.trim();

    if (iContent.length() == 0) {
      return iRecord;
    }

    // UNMARSHALL THE CLASS NAME
    final var record = (EntityImpl) iRecord;

    int pos;
    final var posFirstValue = iContent.indexOf(StringSerializerHelper.ENTRY_SEPARATOR);
    pos = iContent.indexOf(StringSerializerHelper.CLASS_SEPARATOR);
    if (pos > -1 && (pos < posFirstValue || posFirstValue == -1)) {
      if ((record.getIdentity().getClusterId() < 0 || session == null)) {
        EntityInternalUtils.fillClassNameIfNeeded(((EntityImpl) iRecord),
            iContent.substring(0, pos));
      }
      iContent = iContent.substring(pos + 1);
    } else {
      record.setClassNameIfExists(null);
    }

    if (iFields != null && iFields.length == 1 && iFields[0].equals("@class"))
    // ONLY THE CLASS NAME HAS BEEN REQUESTED: RETURN NOW WITHOUT UNMARSHALL THE ENTIRE RECORD
    {
      return iRecord;
    }

    final var fields =
        StringSerializerHelper.smartSplit(
            iContent, StringSerializerHelper.RECORD_SEPARATOR, true, true);

    String fieldName = null;
    String fieldValue;
    PropertyType type;
    SchemaClass linkedClass;
    PropertyType linkedType;
    SchemaProperty prop;

    final Set<String> fieldSet;

    if (iFields != null && iFields.length > 0) {
      fieldSet = new HashSet<String>(iFields.length);
      Collections.addAll(fieldSet, iFields);
    } else {
      fieldSet = null;
    }

    // UNMARSHALL ALL THE FIELDS
    for (var fieldEntry : fields) {
      fieldEntry = fieldEntry.trim();
      var uncertainType = false;

      try {
        pos = fieldEntry.indexOf(FIELD_VALUE_SEPARATOR);
        if (pos > -1) {
          // GET THE FIELD NAME
          fieldName = fieldEntry.substring(0, pos);

          // CHECK IF THE FIELD IS REQUESTED TO BEING UNMARSHALLED
          if (fieldSet != null && !fieldSet.contains(fieldName)) {
            continue;
          }

          if (record.containsField(fieldName))
          // ALREADY UNMARSHALLED: DON'T OVERWRITE IT
          {
            continue;
          }

          // GET THE FIELD VALUE
          fieldValue = fieldEntry.length() > pos + 1 ? fieldEntry.substring(pos + 1) : null;

          var setFieldType = false;

          // SEARCH FOR A CONFIGURED PROPERTY
          SchemaImmutableClass result1 = null;
          if (record != null) {
            result1 = record.getImmutableSchemaClass(session);
          }
          if (result1 != null) {
            SchemaImmutableClass result = null;
            if (record != null) {
              result = record.getImmutableSchemaClass(session);
            }
            prop = result
                .getProperty(session, fieldName);
          } else {
            prop = null;
          }
          if (prop != null && prop.getType(session) != PropertyType.ANY) {
            // RECOGNIZED PROPERTY
            type = prop.getType(session);
            linkedClass = prop.getLinkedClass(session);
            linkedType = prop.getLinkedType(session);

          } else {
            // SCHEMA PROPERTY NOT FOUND FOR THIS FIELD: TRY TO AUTODETERMINE THE BEST TYPE
            type = record.getPropertyType(fieldName);
            if (type == PropertyType.ANY) {
              type = null;
            }
            if (type != null) {
              setFieldType = true;
            }
            linkedClass = null;
            linkedType = null;

            // NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
            if (fieldValue != null && type == null) {
              if (fieldValue.length() > 1
                  && fieldValue.charAt(0) == '"'
                  && fieldValue.charAt(fieldValue.length() - 1) == '"') {
                type = PropertyType.STRING;
              } else if (fieldValue.startsWith(StringSerializerHelper.LINKSET_PREFIX)) {
                type = PropertyType.LINKSET;
              } else if (fieldValue.charAt(0) == StringSerializerHelper.LIST_BEGIN
                  && fieldValue.charAt(fieldValue.length() - 1)
                  == StringSerializerHelper.LIST_END
                  || fieldValue.charAt(0) == StringSerializerHelper.SET_BEGIN
                  && fieldValue.charAt(fieldValue.length() - 1)
                  == StringSerializerHelper.SET_END) {
                // EMBEDDED LIST/SET
                type =
                    fieldValue.charAt(0) == StringSerializerHelper.LIST_BEGIN
                        ? PropertyType.EMBEDDEDLIST
                        : PropertyType.EMBEDDEDSET;

                final var value = fieldValue.substring(1, fieldValue.length() - 1);

                if (!value.isEmpty()) {
                  if (value.charAt(0) == StringSerializerHelper.LINK) {
                    // TODO replace with regex
                    // ASSURE ALL THE ITEMS ARE RID
                    var max = value.length();
                    var allLinks = true;
                    var checkRid = true;
                    for (var i = 0; i < max; ++i) {
                      var c = value.charAt(i);
                      if (checkRid) {
                        if (c != '#') {
                          allLinks = false;
                          break;
                        }
                        checkRid = false;
                      } else if (c == ',') {
                        checkRid = true;
                      }
                    }

                    if (allLinks) {
                      type =
                          fieldValue.charAt(0) == StringSerializerHelper.LIST_BEGIN
                              ? PropertyType.LINKLIST
                              : PropertyType.LINKSET;
                      linkedType = PropertyType.LINK;
                    }
                  } else if (value.charAt(0) == StringSerializerHelper.EMBEDDED_BEGIN) {
                    linkedType = PropertyType.EMBEDDED;
                  } else if (Character.isDigit(value.charAt(0))
                      || value.charAt(0) == '+'
                      || value.charAt(0) == '-') {
                    var items = value.split(",");
                    linkedType = getType(items[0]);
                  } else if (value.charAt(0) == '\'' || value.charAt(0) == '"') {
                    linkedType = PropertyType.STRING;
                  }
                } else {
                  uncertainType = true;
                }

              } else if (fieldValue.charAt(0) == StringSerializerHelper.MAP_BEGIN
                  && fieldValue.charAt(fieldValue.length() - 1)
                  == StringSerializerHelper.MAP_END) {
                type = PropertyType.EMBEDDEDMAP;
              } else if (fieldValue.charAt(0) == StringSerializerHelper.LINK) {
                type = PropertyType.LINK;
              } else if (fieldValue.charAt(0) == StringSerializerHelper.EMBEDDED_BEGIN) {
                // TEMPORARY PATCH
                if (fieldValue.startsWith("(ORIDs")) {
                  type = PropertyType.LINKSET;
                } else {
                  type = PropertyType.EMBEDDED;
                }
              } else if (fieldValue.charAt(0) == StringSerializerHelper.BAG_BEGIN) {
                type = PropertyType.LINKBAG;
              } else if (fieldValue.equals("true") || fieldValue.equals("false")) {
                type = PropertyType.BOOLEAN;
              } else {
                type = getType(fieldValue);
              }
            }
          }
          final var value =
              fieldFromStream(session, iRecord, type, linkedClass, linkedType, fieldName,
                  fieldValue);
          if ("@class".equals(fieldName)) {
            EntityInternalUtils.fillClassNameIfNeeded(((EntityImpl) iRecord), value.toString());
          } else {
            record.field(fieldName, value, type);
          }

          if (uncertainType) {
            record.setFieldType(fieldName, null);
          }
        }
      } catch (Exception e) {
        throw BaseException.wrapException(
            new SerializationException(session,
                "Error on unmarshalling field '"
                    + fieldName
                    + "' in record "
                    + iRecord.getIdentity()
                    + " with value: "
                    + fieldEntry),
            e, session);
      }
    }

    return iRecord;
  }

  @Override
  public byte[] toStream(DatabaseSessionInternal session, RecordAbstract iRecord) {
    final var result = super.toStream(session, iRecord);
    if (result == null || result.length > 0) {
      return result;
    }

    // Fix of nasty IBM JDK bug. In case of very depth recursive graph serialization
    // EntityImpl#_source property may be initialized incorrectly.
    final var recordSchemaAware = (EntityImpl) iRecord;
    if (recordSchemaAware.fields() > 0) {
      return null;
    }

    return result;
  }

  @Override
  protected StringWriter toString(
      DatabaseSessionInternal session, DBRecord iRecord,
      final StringWriter iOutput,
      final String iFormat,
      final boolean autoDetectCollectionType) {
    if (iRecord == null) {
      throw new SerializationException(session, "Expected a record but was null");
    }

    if (!(iRecord instanceof EntityImpl record)) {
      throw new SerializationException(session,
          "Cannot marshall a record of type " + iRecord.getClass().getSimpleName());
    }

    SchemaImmutableClass result2 = null;
    if (record != null) {
      result2 = record.getImmutableSchemaClass(session);
    }
    if (result2 != null) {
      SchemaImmutableClass result = null;
      if (record != null) {
        result = record.getImmutableSchemaClass(session);
      }
      iOutput.append(
          result.getStreamableName(session));
      iOutput.append(StringSerializerHelper.CLASS_SEPARATOR);
    }

    SchemaProperty prop;
    PropertyType type;
    SchemaClass linkedClass;
    PropertyType linkedType;
    String fieldClassName;
    var i = 0;

    final var fieldNames = record.fieldNames();

    // MARSHALL ALL THE FIELDS OR DELTA IF TRACKING IS ENABLED
    for (var fieldName : fieldNames) {
      var fieldValue = record.rawField(fieldName);
      if (i > 0) {
        iOutput.append(StringSerializerHelper.RECORD_SEPARATOR);
      }

      // SEARCH FOR A CONFIGURED PROPERTY
      SchemaImmutableClass result1 = null;
      if (record != null) {
        result1 = record.getImmutableSchemaClass(session);
      }
      if (result1 != null) {
        SchemaImmutableClass result = null;
        if (record != null) {
          result = record.getImmutableSchemaClass(session);
        }
        prop = result.getProperty(session, fieldName);
      } else {
        prop = null;
      }
      fieldClassName = getClassName(fieldValue);

      type = record.getPropertyType(fieldName);
      if (type == PropertyType.ANY) {
        type = null;
      }

      linkedClass = null;
      linkedType = null;

      if (prop != null && prop.getType(session) != PropertyType.ANY) {
        // RECOGNIZED PROPERTY
        type = prop.getType(session);
        linkedClass = prop.getLinkedClass(session);
        linkedType = prop.getLinkedType(session);

      } else if (fieldValue != null) {
        // NOT FOUND: TRY TO DETERMINE THE TYPE FROM ITS CONTENT
        if (type == null) {
          if (fieldValue.getClass() == byte[].class) {
            type = PropertyType.BINARY;
          } else if (fieldValue instanceof DBRecord) {
            if (fieldValue instanceof EntityImpl && ((EntityImpl) fieldValue).hasOwners()) {
              type = PropertyType.EMBEDDED;
            } else {
              type = PropertyType.LINK;
            }

            linkedClass = getLinkInfo(session, fieldClassName);
          } else if (fieldValue instanceof RID)
          // DETERMINE THE FIELD TYPE
          {
            type = PropertyType.LINK;
          } else if (fieldValue instanceof Date) {
            type = PropertyType.DATETIME;
          } else if (fieldValue instanceof String) {
            type = PropertyType.STRING;
          } else if (fieldValue instanceof Integer || fieldValue instanceof BigInteger) {
            type = PropertyType.INTEGER;
          } else if (fieldValue instanceof Long) {
            type = PropertyType.LONG;
          } else if (fieldValue instanceof Float) {
            type = PropertyType.FLOAT;
          } else if (fieldValue instanceof Short) {
            type = PropertyType.SHORT;
          } else if (fieldValue instanceof Byte) {
            type = PropertyType.BYTE;
          } else if (fieldValue instanceof Double) {
            type = PropertyType.DOUBLE;
          } else if (fieldValue instanceof BigDecimal) {
            type = PropertyType.DECIMAL;
          } else if (fieldValue instanceof RidBag) {
            type = PropertyType.LINKBAG;
          }

          if (fieldValue instanceof MultiCollectionIterator<?>) {
            type =
                ((MultiCollectionIterator<?>) fieldValue).isEmbedded()
                    ? PropertyType.EMBEDDEDLIST
                    : PropertyType.LINKLIST;
            linkedType =
                ((MultiCollectionIterator<?>) fieldValue).isEmbedded()
                    ? PropertyType.EMBEDDED
                    : PropertyType.LINK;
          } else if (fieldValue instanceof Collection<?> || fieldValue.getClass().isArray()) {
            final var size = MultiValue.getSize(fieldValue);

            if (autoDetectCollectionType) {
              if (size > 0) {
                final var firstValue = MultiValue.getFirstValue(fieldValue);

                if (firstValue != null) {
                  if (firstValue instanceof RID) {
                    linkedClass = null;
                    linkedType = PropertyType.LINK;
                    if (fieldValue instanceof Set<?>) {
                      type = PropertyType.LINKSET;
                    } else {
                      type = PropertyType.LINKLIST;
                    }
                  } else if ((firstValue instanceof EntityImpl
                      && !((EntityImpl) firstValue).isEmbedded())
                      && (firstValue instanceof DBRecord)) {
                    linkedClass =
                        getLinkInfo(
                            session, getClassName(firstValue));
                    if (type == null) {
                      // LINK: GET THE CLASS
                      linkedType = PropertyType.LINK;

                      if (fieldValue instanceof Set<?>) {
                        type = PropertyType.LINKSET;
                      } else {
                        type = PropertyType.LINKLIST;
                      }
                    } else {
                      linkedType = PropertyType.EMBEDDED;
                    }
                  } else {
                    // EMBEDDED COLLECTION
                    if (firstValue instanceof EntityImpl
                        && ((((EntityImpl) firstValue).hasOwners())
                        || type == PropertyType.EMBEDDEDSET
                        || type == PropertyType.EMBEDDEDLIST
                        || type == PropertyType.EMBEDDEDMAP)) {
                      linkedType = PropertyType.EMBEDDED;
                    } else if (firstValue instanceof Enum<?>) {
                      linkedType = PropertyType.STRING;
                    } else {
                      linkedType = PropertyType.getTypeByClass(firstValue.getClass());

                      if (linkedType != PropertyType.LINK)
                      // EMBEDDED FOR SURE DON'T USE THE LINKED TYPE
                      {
                        linkedType = null;
                      }
                    }

                    if (type == null) {
                      if (fieldValue instanceof LinkSet) {
                        type = PropertyType.LINKSET;
                      } else if (fieldValue instanceof Set<?>) {
                        type = PropertyType.EMBEDDEDSET;
                      } else {
                        type = PropertyType.EMBEDDEDLIST;
                      }
                    }
                  }
                }
              } else if (type == null) {
                type = PropertyType.EMBEDDEDLIST;
              }
            }
          } else if (fieldValue instanceof Map<?, ?> && type == null) {
            final var size = MultiValue.getSize(fieldValue);

            if (size > 0) {
              final var firstValue = MultiValue.getFirstValue(fieldValue);

              if (firstValue != null) {
                if ((firstValue instanceof EntityImpl
                    && !((EntityImpl) firstValue).isEmbedded())
                    && (firstValue instanceof DBRecord)) {
                  linkedClass =
                      getLinkInfo(session, getClassName(firstValue));
                  // LINK: GET THE CLASS
                  linkedType = PropertyType.LINK;
                  type = PropertyType.LINKMAP;
                }
              }
            }

            if (type == null) {
              type = PropertyType.EMBEDDEDMAP;
            }
          }
        }
      }

      if (type == null) {
        type = PropertyType.EMBEDDED;
      }

      iOutput.append(fieldName);
      iOutput.append(FIELD_VALUE_SEPARATOR);
      fieldToStream(session, record, iOutput, type, linkedClass, linkedType, fieldName, fieldValue);

      i++;
    }

    return iOutput;
  }

  private String getClassName(final Object iValue) {
    if (iValue instanceof EntityImpl) {
      return ((EntityImpl) iValue).getSchemaClassName();
    }

    return iValue != null ? iValue.getClass().getSimpleName() : null;
  }

  private SchemaClass getLinkInfo(
      final DatabaseSessionInternal iDatabase, final String iFieldClassName) {
    if (iDatabase == null || iDatabase.isClosed() || iFieldClassName == null) {
      return null;
    }

    var linkedClass =
        iDatabase.getMetadata().getImmutableSchemaSnapshot().getClass(iFieldClassName);

    return linkedClass;
  }
}
