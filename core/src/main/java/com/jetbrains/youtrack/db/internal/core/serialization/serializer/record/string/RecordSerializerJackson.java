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

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.core.type.TypeReference;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchHelper;
import com.jetbrains.youtrack.db.internal.core.fetch.FetchPlan;
import com.jetbrains.youtrack.db.internal.core.fetch.json.JSONFetchContext;
import com.jetbrains.youtrack.db.internal.core.fetch.json.JSONFetchListener;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.RecordStringable;
import com.jetbrains.youtrack.db.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.JSONWriter;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJSON.FormatSettings;
import java.io.IOException;
import java.io.StringWriter;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordSerializerJackson extends RecordSerializerStringAbstract {

  public static final String NAME = "jackson";
  public static final RecordSerializerJackson INSTANCE = new RecordSerializerJackson();

  @Override
  public int getCurrentVersion() {
    return 0;
  }

  @Override
  public int getMinSupportedVersion() {
    return 0;
  }

  @Override
  public RecordAbstract fromString(DatabaseSessionInternal db, String source,
      RecordAbstract record, final String[] fields) {
    JsonFactory jsonFactory = new JsonFactory();
    try {
      String className = null;
      Map<String, String> fieldTypes = null;

      try (JsonParser jsonParser = jsonFactory.createParser(source)) {
        var token = jsonParser.nextToken();
        if (token != JsonToken.START_OBJECT) {
          throw new SerializationException("Invalid JSON content: " + source);
        }

        int metaFilesProcessed = 0;
        while (metaFilesProcessed < 3 && token != JsonToken.END_OBJECT) {
          token = jsonParser.nextToken();
          if (token == JsonToken.FIELD_NAME) {
            var fieldName = jsonParser.currentName();

            if (fieldName.equals(FieldTypesString.ATTRIBUTE_FIELD_TYPES)) {
              metaFilesProcessed++;
              if (record instanceof EntityImpl) {
                fieldTypes = parseFieldTypes(jsonParser);
              }
            } else {
              if (fieldName.equals(EntityHelper.ATTRIBUTE_TYPE)) {
                metaFilesProcessed++;
                var fieldValueAsString = jsonParser.nextTextValue();
                if (fieldValueAsString.length() != 1) {
                  throw new SerializationException(
                      "Invalid record type: " + fieldValueAsString + " for record: " + source);
                }

                if (record == null) {
                  // CREATE THE RIGHT RECORD INSTANCE
                  record =
                      YouTrackDBEnginesManager.instance()
                          .getRecordFactoryManager()
                          .newInstance(
                              (byte) fieldValueAsString.charAt(0),
                              new ChangeableRecordId(),
                              db);
                } else if (record.getRecordType() != fieldValueAsString.charAt(0)) {
                  throw new SerializationException(
                      "Record type mismatch: " + fieldValueAsString + " != "
                          + record.getRecordType());
                }
              } else {
                if (fieldName.equals(EntityHelper.ATTRIBUTE_RID)) {
                  metaFilesProcessed++;
                  var fieldValueAsString = jsonParser.nextTextValue();
                  if (!fieldValueAsString.isEmpty()) {
                    var recordId = new RecordId(fieldValueAsString);
                    if (record != null) {
                      if (!recordId.equals(record.getIdentity())) {
                        throw new SerializationException(
                            "Record ID mismatch: " + recordId + " != " + record.getIdentity());
                      }
                    } else {
                      record = db.load(recordId);
                    }
                  }
                } else {
                  if (fieldName.equals(EntityHelper.ATTRIBUTE_CLASS)) {
                    var fieldValueAsString = jsonParser.nextTextValue();
                    className = "null".equals(fieldValueAsString) ? null : fieldValueAsString;
                  }
                }
              }
            }

            token = jsonParser.nextToken();
            if (token == JsonToken.START_ARRAY) {
              // ARRAY
              jsonParser.skipChildren();
            } else {
              if (token == JsonToken.START_OBJECT) {
                // OBJECT
                jsonParser.skipChildren();
              } else {
                // VALUE
                throw new SerializationException("Invalid JSON content: " + source);
              }
            }
          }
        }
      }

      if (record == null) {
        if (className == null) {
          record = db.newInstance();
        } else {
          record = db.newInstance(className);
        }

        RecordInternal.unsetDirty(record);
      }

      try (JsonParser jsonParser = jsonFactory.createParser(source)) {
        var token = jsonParser.nextToken();
        if (token != JsonToken.START_OBJECT) {
          throw new SerializationException("Invalid JSON content: " + source);
        }

        while (token != JsonToken.END_OBJECT) {
          token = jsonParser.nextToken();
          if (token == JsonToken.FIELD_NAME) {
            var fieldName = jsonParser.currentName();
            parseRecord(db, fieldTypes, record, jsonParser, fieldName, source);
          }
        }
      }

      return record;
    } catch (Exception e) {
      if (record.getIdentity().isValid()) {
        throw BaseException.wrapException(
            new SerializationException(
                "Error on unmarshalling JSON content for record " + record.getIdentity()),
            e);
      } else {
        throw BaseException.wrapException(
            new SerializationException(
                "Error on unmarshalling JSON content for record: " + source),
            e);
      }
    }

  }

  private static Map<String, String> parseFieldTypes(JsonParser jsonParser) throws IOException {
    return jsonParser.readValueAs(new TypeReference<Map<String, String>>() {
    });
  }

  private void parseRecord(
      DatabaseSessionInternal db,
      Map<String, String> fieldTypes,
      RecordAbstract record,
      JsonParser jsonParser,
      String fieldName, String source) throws IOException {
    // RECORD ATTRIBUTES
    if (!(record instanceof EntityImpl entity)) {
      if (fieldName.equals("value")) {
        var nextToken = jsonParser.nextToken();
        if (nextToken == JsonToken.VALUE_STRING) {
          var fieldValue = nextToken.asString();
          if ("null".equals(fieldValue)) {
            record.fromStream(CommonConst.EMPTY_BYTE_ARRAY);
          } else {
            if (record instanceof Blob) {
              // BYTES
              RecordInternal.unsetDirty(record);
              RecordInternal.fill(
                  record,
                  record.getIdentity(),
                  record.getVersion(),
                  Base64.getDecoder().decode(fieldValue),
                  true);
            } else {
              if (record instanceof RecordStringable) {
                ((RecordStringable) record).value(fieldValue);
              } else {
                throw new SerializationException(
                    "Unsupported type of record : " + record.getClass().getName());
              }
            }
          }
        } else {
          throw new SerializationException(
              "Expected JSON token is a string token, but found " + nextToken
                  + ". JSON content:\r\n "
                  + source + " Exp");
        }
      } else {
        throw new SerializationException(
            "Expected field -> 'value'. JSON content:\r\n " + source);
      }
    } else {
      var type = determineType(entity, fieldName, fieldTypes.get(fieldName));
      var v = parseValue(db, entity, jsonParser, type, source);

      if (type != null) {
        entity.setPropertyInternal(fieldName, v, type);
      } else {
        entity.setPropertyInternal(fieldName, v);
      }
    }
  }


  public static void toString(DatabaseSessionInternal db, final Record iRecord,
      final JSONWriter json,
      final String iFormat) {
    try {
      final FormatSettings settings = new FormatSettings(iFormat);
      json.beginObject();

      JSONFetchContext context = new JSONFetchContext(json, settings);
      context.writeSignature(db, json, iRecord);

      if (iRecord instanceof EntityImpl) {
        final FetchPlan fp = FetchHelper.buildFetchPlan(settings.fetchPlan);

        FetchHelper.fetch(db, iRecord, null, fp, new JSONFetchListener(), context, iFormat);
      } else {
        if (iRecord instanceof RecordStringable record) {
          json.writeAttribute(db, settings.indentLevel, true, "value", record.value());
        } else {
          if (iRecord instanceof Blob record) {
            json.writeAttribute(db,
                settings.indentLevel,
                true,
                "value",
                Base64.getEncoder().encodeToString(((RecordAbstract) record).toStream()));
          } else {
            throw new SerializationException(
                "Error on marshalling record of type '"
                    + iRecord.getClass()
                    + "' to JSON. The record type cannot be exported to JSON");
          }
        }
      }
      json.endObject(settings.indentLevel, true);
    } catch (IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public StringBuilder toString(
      DatabaseSessionInternal db, final Record record,
      final StringBuilder output,
      final String format,
      boolean autoDetectCollectionType) {
    try {
      final StringWriter buffer = new StringWriter(RecordSerializerJSON.INITIAL_SIZE);
      final JSONWriter json = new JSONWriter(buffer, format);
      final FormatSettings settings = new FormatSettings(format);

      json.beginObject();

      final JSONFetchContext context = new JSONFetchContext(json, settings);
      context.writeSignature(db, json, record);
      if (record instanceof EntityImpl) {
        final FetchPlan fp = FetchHelper.buildFetchPlan(settings.fetchPlan);
        FetchHelper.fetch(db, record, null, fp, new JSONFetchListener(), context, format);
      } else {
        if (record instanceof RecordStringable recordStringable) {
          // STRINGABLE
          json.writeAttribute(db, settings.indentLevel, true, "value", recordStringable.value());
        } else {
          if (record instanceof Blob recordBlob) {
            // BYTES
            json.writeAttribute(db,
                settings.indentLevel,
                true,
                "value",
                Base64.getEncoder().encodeToString(((RecordAbstract) recordBlob).toStream()));
          } else {
            throw new SerializationException(
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
      throw BaseException.wrapException(
          new SerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  @Override
  public String toString() {
    return NAME;
  }

  @Nullable
  private static PropertyType determineType(EntityImpl entity, String fieldName,
      String charType) {
    PropertyType type = null;

    final SchemaClass cls = EntityInternalUtils.getImmutableSchemaClass(entity);
    if (cls != null) {
      final Property prop = cls.getProperty(fieldName);
      if (prop != null) {
        type = prop.getType();
      }
    }

    if (type != null) {
      return type;
    }

    type = switch (charType) {
      case "f" -> PropertyType.FLOAT;
      case "c" -> PropertyType.DECIMAL;
      case "l" -> PropertyType.LONG;
      case "b" -> PropertyType.BINARY;
      case "d" -> PropertyType.DOUBLE;
      case "a" -> PropertyType.DATE;
      case "t" -> PropertyType.DATETIME;
      case "s" -> PropertyType.SHORT;
      case "e" -> PropertyType.EMBEDDEDSET;
      case "g" -> PropertyType.LINKBAG;
      case "z" -> PropertyType.LINKLIST;
      case "m" -> PropertyType.LINKMAP;
      case "x" -> PropertyType.LINK;
      case "n" -> PropertyType.LINKSET;
      case "u" -> PropertyType.CUSTOM;

      default -> null;
    };

    if (type != null) {
      return type;
    }

    return entity.getPropertyType(fieldName);
  }

  @Nullable
  private Object parseValue(
      @Nonnull DatabaseSessionInternal db,
      @Nullable final EntityImpl entity,
      @Nonnull JsonParser jsonParser,
      @Nullable PropertyType type,
      @Nonnull String source) throws IOException {
    var nextToken = jsonParser.nextToken();
    return switch (nextToken) {
      case VALUE_NULL -> null;

      case VALUE_STRING -> {
        if (type == null) {
          var text = jsonParser.getText();
          if (StringSerializerHelper.SKIPPED_VALUE.equals(text)) {
            yield new RidBag(db);
          }

          yield jsonParser.getText();
        } else {
          if (type == PropertyType.BINARY) {
            var text = jsonParser.getText();
            if (!text.isEmpty() && text.length() <= 3) {
              yield PropertyType.convert(db, text, Byte.class);
            }

            yield Base64.getDecoder().decode(text);
          } else {
            yield PropertyType.convert(db, jsonParser.getText(), type.getDefaultJavaType());
          }
        }
      }

      case VALUE_NUMBER_INT -> jsonParser.getIntValue();
      case VALUE_FALSE -> false;
      case VALUE_TRUE -> true;
      case VALUE_NUMBER_FLOAT -> jsonParser.getFloatValue();

      case START_ARRAY -> switch (type) {
        case EMBEDDEDLIST -> parseEmbeddedList(db, entity, jsonParser, source);
        case LINKLIST -> parseLinkList(entity, jsonParser);
        case LINKSET -> parseLinkSet(entity, jsonParser);
        case EMBEDDEDSET -> parseEmbeddedSet(db, entity, jsonParser, source);
        case null -> parseEmbeddedList(db, entity, jsonParser, source);
        default -> throw new SerializationException("Invalid JSON content: " + source);
      };

      case START_OBJECT -> switch (type) {
        case EMBEDDED -> parseEmbeddedEntity(db, jsonParser, source);
        case EMBEDDEDMAP -> parseEmbeddedMap(db, entity, jsonParser, source);

        case null -> {
          var value = parseEmbeddedMap(db, entity, jsonParser, source);

          if (value.containsKey(EntityHelper.ATTRIBUTE_CLASS)) {
            var embedded = db.newEmbededEntity(value.get(EntityHelper.ATTRIBUTE_CLASS).toString());
            embedded.updateFromMap(value);
            yield embedded;
          } else {
            yield value;
          }
        }

        default -> throw new SerializationException("Invalid JSON content: " + source);
      };

      default -> throw new SerializationException("Invalid JSON content: " + source);
    };
  }

  private EmbeddedEntityImpl parseEmbeddedEntity(DatabaseSessionInternal db,
      JsonParser jsonParser, String source) throws IOException {
    var embedded = (EmbeddedEntityImpl) db.newEmbededEntity();
    var token = jsonParser.nextToken();

    Map<String, String> fieldTypes = new HashMap<>();
    while (token != JsonToken.END_OBJECT) {
      token = jsonParser.nextToken();
      if (token == JsonToken.FIELD_NAME) {
        var fieldName = jsonParser.currentName();

        switch (fieldName) {
          case FieldTypesString.ATTRIBUTE_FIELD_TYPES -> {
            fieldTypes = parseFieldTypes(jsonParser);
          }
          case EntityHelper.ATTRIBUTE_CLASS -> {
            var className = jsonParser.nextTextValue();
            embedded.setClazzName(className);
          }

          default -> parseRecord(db, fieldTypes, embedded, jsonParser, fieldName, source);
        }
      }
    }

    return embedded;
  }

  private TrackedMap<Object> parseEmbeddedMap(DatabaseSessionInternal db, EntityImpl entity,
      JsonParser jsonParser, String source) throws IOException {
    var map = new TrackedMap<>(entity);
    jsonParser.nextToken();

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      var fieldName = jsonParser.currentName();
      var value = parseValue(db, null, jsonParser, null, source);
      map.put(fieldName, value);
    }

    return map;
  }

  private static LinkList parseLinkList(EntityImpl entity, JsonParser jsonParser)
      throws IOException {
    var list = new LinkList(entity);
    jsonParser.nextToken();

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      var ridText = jsonParser.getText();
      list.add(new RecordId(ridText));
    }

    return list;
  }

  private static LinkSet parseLinkSet(EntityImpl entity, JsonParser jsonParser)
      throws IOException {
    var list = new LinkSet(entity);
    jsonParser.nextToken();

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      var ridText = jsonParser.getText();
      list.add(new RecordId(ridText));
    }

    return list;
  }

  private TrackedList<Object> parseEmbeddedList(DatabaseSessionInternal db, EntityImpl entity,
      JsonParser jsonParser, String source) throws IOException {
    var list = new TrackedList<>(entity);
    jsonParser.nextToken();

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      list.add(parseValue(db, null, jsonParser, null, source));
    }

    return list;
  }

  private TrackedSet<Object> parseEmbeddedSet(DatabaseSessionInternal db, EntityImpl entity,
      JsonParser jsonParser, String source) throws IOException {
    var list = new TrackedSet<>(entity);
    jsonParser.nextToken();

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      list.add(parseValue(db, null, jsonParser, null, source));
    }

    return list;
  }

  @Override
  public String getName() {
    return NAME;
  }
}
