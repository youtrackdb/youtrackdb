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
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.record.Blob;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkList;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkMap;
import com.jetbrains.youtrack.db.internal.core.db.record.LinkSet;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedList;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedMap;
import com.jetbrains.youtrack.db.internal.core.db.record.TrackedSet;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordAbstract;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EmbeddedEntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityHelper;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityInternalUtils;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerJSON.FormatSettings;
import java.io.IOException;
import java.io.StringWriter;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public class RecordSerializerJackson extends RecordSerializerStringAbstract {

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
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
  public <T extends Record> T fromString(DatabaseSessionInternal db, String source,
      RecordAbstract record, final String[] fields) {
    try (JsonParser jsonParser = JSON_FACTORY.createParser(source)) {
      //noinspection unchecked
      return (T) recordFromJson(db, record, jsonParser);
    } catch (Exception e) {
      if (record != null && record.getIdentity().isValid()) {
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

  private static RecordAbstract recordFromJson(
      @Nonnull DatabaseSessionInternal db,
      @Nullable RecordAbstract record,
      @Nonnull JsonParser jsonParser) throws IOException {
    var token = jsonParser.nextToken();
    if (token != JsonToken.START_OBJECT) {
      throw new SerializationException("Start of the object is expected");
    }

    String defaultClassName;

    byte defaultRecordType = record != null ? record.getRecordType() : EntityImpl.RECORD_TYPE;
    if (record instanceof EntityImpl entity) {
      defaultClassName = entity.getClassName();
    } else if (defaultRecordType == EntityImpl.RECORD_TYPE) {
      defaultClassName = Entity.DEFAULT_CLASS_NAME;
    } else {
      defaultClassName = null;
    }

    var recordMetaData = parseRecordMetadata(jsonParser, defaultClassName, defaultRecordType);
    if (recordMetaData == null) {
      recordMetaData = new RecordMetaData(defaultRecordType,
          record != null ? record.getIdentity() : null,
          defaultClassName, Collections.emptyMap());
    }

    var result = createRecordFromJsonAfterMetadata(db, record, recordMetaData, jsonParser);
    if (jsonParser.nextToken() != null) {
      throw new SerializationException("End of the JSON object is expected");
    }

    return result;
  }

  private static RecordAbstract createRecordFromJsonAfterMetadata(DatabaseSessionInternal db,
      RecordAbstract record,
      RecordMetaData recordMetaData, JsonParser jsonParser) throws IOException {
    //initialize record first and then validate the rest of the found metadata
    if (record == null) {
      if (recordMetaData.recordId != null) {
        record = db.load(recordMetaData.recordId);
      } else {
        if (recordMetaData.recordType == EntityImpl.RECORD_TYPE) {
          if (recordMetaData.className == null) {
            record = db.newInstance();
          } else {
            record = db.newInstance(recordMetaData.className);
          }
        } else if (recordMetaData.recordType == Blob.RECORD_TYPE) {
          record = (RecordAbstract) db.newBlob();
        } else {
          throw new SerializationException(
              "Unsupported record type: " + recordMetaData.recordType);
        }
      }

      RecordInternal.unsetDirty(record);
    }

    if (record.getRecordType() != recordMetaData.recordType) {
      throw new SerializationException(
          "Record type mismatch: " + record.getRecordType() + " != " + recordMetaData.recordType);
    }
    if (record instanceof EntityImpl entity && !Objects.equals(entity.getClassName(),
        recordMetaData.className)) {
      throw new SerializationException(
          "Record class name mismatch: " + entity.getClassName() + " != "
              + recordMetaData.className);
    }
    if (recordMetaData.recordId != null && !record.getIdentity().equals(recordMetaData.recordId)) {
      throw new SerializationException(
          "Record id mismatch: " + record.getIdentity() + " != " + recordMetaData.recordId);
    }

    parseProperties(db, record, recordMetaData, jsonParser);
    return record;
  }

  private static void parseProperties(DatabaseSessionInternal db, RecordAbstract record,
      RecordMetaData recordMetaData, JsonParser jsonParser) throws IOException {
    JsonToken token;
    token = jsonParser.currentToken();

    while (token != JsonToken.END_OBJECT) {
      if (token == JsonToken.FIELD_NAME) {
        var fieldName = jsonParser.currentName();
        if (!fieldName.isEmpty() && fieldName.charAt(0) == '@') {
          throw new SerializationException("Invalid property name: " + fieldName);
        }

        jsonParser.nextToken();//jump to value
        parseProperty(db, recordMetaData.fieldTypes, record, jsonParser, fieldName);
      } else {
        throw new SerializationException("Expected field name");
      }
      token = jsonParser.nextToken();
    }
  }

  @Nullable
  private static RecordMetaData parseRecordMetadata(@Nullable JsonParser jsonParser,
      @Nullable String defaultClassName, byte defaultRecordType) throws IOException {
    var token = jsonParser.nextToken();
    RecordId recordId = null;
    byte recordType = defaultRecordType;
    String className = defaultClassName;
    Map<String, String> fieldTypes = new HashMap<>();

    int fieldsCount = 0;
    while (token != JsonToken.END_OBJECT) {
      if (token == JsonToken.FIELD_NAME) {
        var fieldName = jsonParser.currentName();
        if (fieldName.charAt(0) != '@') {
          break;
        }

        fieldsCount++;
        switch (fieldName) {
          case FieldTypesString.ATTRIBUTE_FIELD_TYPES -> {
            fieldTypes = parseFieldTypes(jsonParser);
            token = jsonParser.nextToken();
          }
          case EntityHelper.ATTRIBUTE_TYPE -> {
            token = jsonParser.nextToken();
            if (token != JsonToken.VALUE_STRING) {
              throw new SerializationException(
                  "Expected field value as string.");
            }
            var fieldValueAsString = jsonParser.getText();
            if (fieldValueAsString.length() != 1) {
              throw new SerializationException(
                  "Invalid record type: " + fieldValueAsString);
            }
            recordType = (byte) fieldValueAsString.charAt(0);
            token = jsonParser.nextToken();
          }
          case EntityHelper.ATTRIBUTE_RID -> {
            token = jsonParser.nextToken();
            if (token != JsonToken.VALUE_STRING) {
              throw new SerializationException(
                  "Expected field value as string");
            }
            var fieldValueAsString = jsonParser.getText();
            if (!fieldValueAsString.isEmpty()) {
              recordId = new RecordId(fieldValueAsString);
            }
            token = jsonParser.nextToken();
          }
          case EntityHelper.ATTRIBUTE_CLASS -> {
            token = jsonParser.nextToken();
            if (token != JsonToken.VALUE_STRING) {
              throw new SerializationException(
                  "Expected field value as string");
            }
            var fieldValueAsString = jsonParser.getText();
            className = "null".equals(fieldValueAsString) ? null : fieldValueAsString;
            token = jsonParser.nextToken();
          }
        }
      } else {
        throw new SerializationException("Expected field name");
      }
    }

    if (fieldsCount == 0) {
      return null;
    }
    return new RecordMetaData(recordType, recordId, className, fieldTypes);
  }

  private static Map<String, String> parseFieldTypes(JsonParser jsonParser) throws IOException {
    var map = new HashMap<String, String>();

    var token = jsonParser.nextToken();
    if (token != JsonToken.START_OBJECT) {
      throw new SerializationException("Expected start of object");
    }

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      var fieldName = jsonParser.currentName();
      token = jsonParser.nextToken();
      if (token != JsonToken.VALUE_STRING) {
        throw new SerializationException("Expected field value as string");
      }

      map.put(fieldName, jsonParser.getText());
    }

    return map;
  }

  private static void parseProperty(
      DatabaseSessionInternal db,
      Map<String, String> fieldTypes,
      RecordAbstract record,
      JsonParser jsonParser,
      String fieldName) throws IOException {
    // RECORD ATTRIBUTES
    if (!(record instanceof EntityImpl entity)) {
      if (fieldName.equals("value")) {
        var nextToken = jsonParser.nextToken();
        if (nextToken == JsonToken.VALUE_STRING) {
          var fieldValue = nextToken.asString();
          if ("null".equals(fieldValue)) {
            record.fromStream(CommonConst.EMPTY_BYTE_ARRAY);
          } else if (record instanceof Blob) {
            // BYTES
            RecordInternal.unsetDirty(record);
            RecordInternal.fill(
                record,
                record.getIdentity(),
                record.getVersion(),
                Base64.getDecoder().decode(fieldValue),
                true);
          } else {
            throw new SerializationException(
                "Unsupported type of record : " + record.getClass().getName());
          }
        } else {
          throw new SerializationException(
              "Expected JSON token is a string token, but found " + nextToken);
        }
      } else {
        throw new SerializationException(
            "Expected field -> 'value'. JSON content");
      }
    } else {
      var type = determineType(entity, fieldName, fieldTypes.get(fieldName));
      var v = parseValue(db, entity, jsonParser, type);

      if (type != null) {
        entity.setPropertyInternal(fieldName, v, type);
      } else {
        entity.setPropertyInternal(fieldName, v);
      }
    }
  }

  @Override
  public StringWriter toString(
      DatabaseSessionInternal db, final Record record,
      final StringWriter output,
      final String format,
      boolean autoDetectCollectionType) {
    try (var jsonGenerator = JSON_FACTORY.createGenerator(output)) {
      final FormatSettings settings = new FormatSettings(format);
      recordToJson(record, jsonGenerator, settings);
      return output;
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  public static void recordToJson(Record record, JsonGenerator jsonGenerator,
      @Nullable String format) {
    try {
      final FormatSettings settings = new FormatSettings(format);
      recordToJson(record, jsonGenerator, settings);
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  private static void recordToJson(Record record, JsonGenerator jsonGenerator,
      FormatSettings formatSettings) throws IOException {
    jsonGenerator.writeStartObject();
    writeMetadata(jsonGenerator, (RecordAbstract) record, formatSettings);

    if (record instanceof EntityImpl entity) {
      for (var propertyName : entity.getPropertyNamesInternal()) {
        jsonGenerator.writeFieldName(propertyName);
        var propertyValue = entity.getPropertyInternal(propertyName);

        serializeValue(jsonGenerator, propertyValue, formatSettings);
      }
    } else if (record instanceof Blob recordBlob) {
      // BYTES
      jsonGenerator.writeFieldName("value");
      jsonGenerator.writeBinary(((RecordAbstract) recordBlob).toStream());
    } else {
      throw new SerializationException(
          "Error on marshalling record of type '"
              + record.getClass()
              + "' to JSON. The record type cannot be exported to JSON");
    }
    jsonGenerator.writeEndObject();
  }


  private static void serializeLink(JsonGenerator jsonGenerator, RID rid)
      throws IOException {
    if (!rid.isPersistent()) {
      throw new SerializationException(
          "Cannot serialize non-persistent link: " + rid);
    }
    jsonGenerator.writeString(rid.toString());
  }

  private static void writeMetadata(JsonGenerator jsonGenerator,
      RecordAbstract record, FormatSettings formatSettings)
      throws IOException {
    if (record instanceof EntityImpl entity) {
      if (!entity.isEmbedded()) {
        if (formatSettings.includeId) {
          jsonGenerator.writeFieldName(EntityHelper.ATTRIBUTE_RID);
          serializeLink(jsonGenerator, entity.getIdentity());
        }
      }

      if (formatSettings.includeType) {
        jsonGenerator.writeFieldName(EntityHelper.ATTRIBUTE_TYPE);
        jsonGenerator.writeString(Character.toString(record.getRecordType()));
      }

      var schemaClass = entity.getSchemaClass();
      if (schemaClass != null && formatSettings.includeClazz) {
        jsonGenerator.writeFieldName(EntityHelper.ATTRIBUTE_CLASS);
        jsonGenerator.writeString(schemaClass.getName());
      }

      if (formatSettings.keepTypes) {
        var fieldTypes = new HashMap<String, String>();
        for (var propertyName : entity.getPropertyNames()) {
          PropertyType type = fetchPropertyType(entity, propertyName,
              schemaClass);

          if (type != null) {
            var charType = charType(type);
            if (charType != null) {
              fieldTypes.put(propertyName, charType);
            }
          }
        }

        jsonGenerator.writeFieldName(FieldTypesString.ATTRIBUTE_FIELD_TYPES);
        jsonGenerator.writeStartObject();
        for (var entry : fieldTypes.entrySet()) {
          jsonGenerator.writeFieldName(entry.getKey());
          jsonGenerator.writeString(entry.getValue());
        }

        jsonGenerator.writeEndObject();
      }
    } else {
      if (formatSettings.includeType) {
        jsonGenerator.writeFieldName(EntityHelper.ATTRIBUTE_TYPE);
        jsonGenerator.writeString(String.valueOf(record.getRecordType()));
      }

      if (formatSettings.includeId) {
        jsonGenerator.writeFieldName(EntityHelper.ATTRIBUTE_RID);
        jsonGenerator.writeString(record.getIdentity().toString());
      }
    }
  }

  private static PropertyType fetchPropertyType(EntityImpl entity, String propertyName,
      SchemaClass schemaClass) {
    PropertyType type = null;
    if (schemaClass != null) {
      var property = schemaClass.getProperty(propertyName);

      if (property != null) {
        type = property.getType();
      }
    }

    if (type == null) {
      type = entity.getPropertyType(propertyName);
    }

    if (type == null) {
      type = PropertyType.getTypeByValue(entity.getPropertyInternal(propertyName));
    }

    return type;
  }

  private static String charType(PropertyType type) {
    return switch (type) {
      case FLOAT -> "f";
      case DECIMAL -> "c";
      case LONG -> "l";
      case BYTE -> "y";
      case BINARY -> "b";
      case DOUBLE -> "d";
      case DATE -> "a";
      case DATETIME -> "t";
      case SHORT -> "s";
      case EMBEDDEDSET -> "e";
      case EMBEDDED -> "w";
      case LINKBAG -> "g";
      case LINKLIST -> "z";
      case LINKMAP -> "m";
      case LINK -> "x";
      case LINKSET -> "n";
      case CUSTOM -> "u";
      default -> null;
    };
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
      case "y" -> PropertyType.BYTE;
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
      case "w" -> PropertyType.EMBEDDED;
      case null -> null;
      default -> throw new IllegalArgumentException("Invalid type: " + charType);
    };

    if (type != null) {
      return type;
    }

    return entity.getPropertyType(fieldName);
  }

  private static void serializeValue(JsonGenerator jsonGenerator, Object propertyValue,
      FormatSettings formatSettings)
      throws IOException {
    if (propertyValue != null) {
      switch (propertyValue) {
        case String string -> jsonGenerator.writeString(string);

        case Integer integer -> jsonGenerator.writeNumber(integer);
        case Long longValue -> jsonGenerator.writeNumber(longValue);
        case Float floatValue -> jsonGenerator.writeNumber(floatValue);
        case Double doubleValue -> jsonGenerator.writeNumber(doubleValue);
        case Short shortValue -> jsonGenerator.writeNumber(shortValue);
        case Byte byteValue -> jsonGenerator.writeNumber(byteValue);
        case BigDecimal bigDecimal -> jsonGenerator.writeNumber(bigDecimal);

        case Boolean booleanValue -> jsonGenerator.writeBoolean(booleanValue);

        case byte[] byteArray -> jsonGenerator.writeBinary(byteArray);
        case Entity entityValue -> {
          if (entityValue.isEmbedded()) {
            recordToJson(entityValue, jsonGenerator, formatSettings);
          } else {
            serializeLink(jsonGenerator, entityValue.getIdentity());
          }
        }

        case RID link -> serializeLink(jsonGenerator, link);
        case LinkList linkList -> {
          jsonGenerator.writeStartArray();
          for (var link : linkList) {
            serializeLink(jsonGenerator, link.getIdentity());
          }
          jsonGenerator.writeEndArray();
        }
        case LinkSet linkSet -> {
          jsonGenerator.writeStartArray();
          for (var link : linkSet) {
            serializeLink(jsonGenerator, link.getIdentity());
          }
          jsonGenerator.writeEndArray();
        }
        case RidBag ridBag -> {
          jsonGenerator.writeStartArray();
          for (var link : ridBag) {
            serializeLink(jsonGenerator, link.getIdentity());
          }
          jsonGenerator.writeEndArray();
        }
        case LinkMap linkMap -> {
          jsonGenerator.writeStartObject();
          for (var entry : linkMap.entrySet()) {
            jsonGenerator.writeFieldName(entry.getKey());
            serializeLink(jsonGenerator, entry.getValue().getIdentity());
          }
          jsonGenerator.writeEndObject();
        }

        case TrackedList<?> trackedList -> {
          jsonGenerator.writeStartArray();
          for (var value : trackedList) {
            serializeValue(jsonGenerator, value, formatSettings);
          }
          jsonGenerator.writeEndArray();
        }
        case TrackedSet<?> trackedSet -> {
          jsonGenerator.writeStartArray();
          for (var value : trackedSet) {
            serializeValue(jsonGenerator, value, formatSettings);
          }
          jsonGenerator.writeEndArray();
        }
        case TrackedMap<?> trackedMap -> {
          serializeEmbeddedMap(jsonGenerator, formatSettings, trackedMap);
        }

        case Date date -> jsonGenerator.writeNumber(date.getTime());
        default -> throw new SerializationException(
            "Error on marshalling of record to JSON. Unsupported value: " + propertyValue);
      }
    } else {
      jsonGenerator.writeNull();
    }
  }

  public static void serializeEmbeddedMap(JsonGenerator jsonGenerator,
      Map<String, ?> trackedMap, String format) {
    try {
      final FormatSettings settings = new FormatSettings(format);
      serializeEmbeddedMap(jsonGenerator, settings, trackedMap);
    } catch (final IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error on marshalling of record to JSON"), e);
    }
  }

  private static void serializeEmbeddedMap(JsonGenerator jsonGenerator,
      FormatSettings formatSettings,
      Map<String, ?> trackedMap) throws IOException {
    jsonGenerator.writeStartObject();
    for (var entry : trackedMap.entrySet()) {
      jsonGenerator.writeFieldName(entry.getKey());
      serializeValue(jsonGenerator, entry.getValue(), formatSettings);
    }
    jsonGenerator.writeEndObject();
  }

  @Nullable
  private static Object parseValue(
      @Nonnull DatabaseSessionInternal db,
      @Nullable final EntityImpl entity,
      @Nonnull JsonParser jsonParser,
      @Nullable PropertyType type) throws IOException {
    var token = jsonParser.currentToken();
    return switch (token) {
      case VALUE_NULL -> null;
      case VALUE_STRING -> {
        yield switch (type) {
          case LINK -> new RecordId(jsonParser.getText());
          case BINARY -> {
            var text = jsonParser.getText();
            if (!text.isEmpty() && text.length() <= 3) {
              yield PropertyType.convert(db, text, Byte.class);
            }

            yield Base64.getDecoder().decode(text);
          }
          case null -> {
            var text = jsonParser.getText();
            if (!text.isEmpty() && text.charAt(0) == '#') {
              try {
                yield new RecordId(text);
              } catch (IllegalArgumentException e) {
                yield text;
              }
            } else {
              yield text;
            }
          }
          default -> PropertyType.convert(db, jsonParser.getText(), type.getDefaultJavaType());
        };
      }

      case VALUE_NUMBER_INT, VALUE_NUMBER_FLOAT -> jsonParser.getNumberValue();
      case VALUE_FALSE -> false;
      case VALUE_TRUE -> true;

      case START_ARRAY -> switch (type) {
        case EMBEDDEDLIST -> parseEmbeddedList(db, entity, jsonParser);
        case EMBEDDEDSET -> parseEmbeddedSet(db, entity, jsonParser);

        case LINKLIST -> parseLinkList(entity, jsonParser);
        case LINKSET -> parseLinkSet(entity, jsonParser);
        case LINKBAG -> parseLinkBag(entity, jsonParser);

        case null -> parseEmbeddedList(db, entity, jsonParser);

        default -> throw new SerializationException("Unexpected value type: " + type);
      };

      case START_OBJECT -> switch (type) {
        case EMBEDDED -> parseEmbeddedEntity(db, jsonParser, null);
        case EMBEDDEDMAP -> parseEmbeddedMap(db, entity, jsonParser, null);
        case LINKMAP -> parseLinkMap(entity, jsonParser);
        case LINK -> recordFromJson(db, null, jsonParser);

        case null -> {
          var recordMetaData = parseRecordMetadata(jsonParser, null,
              EntityImpl.RECORD_TYPE);

          if (recordMetaData != null) {
            if (recordMetaData.recordId == null) {
              if (recordMetaData.className == null) {
                yield parseEmbeddedEntity(db, jsonParser, recordMetaData);
              }
            }

            yield createRecordFromJsonAfterMetadata(db, null, recordMetaData, jsonParser);
          }

          //we have read the filed name already, so we need to read the value
          var map = new TrackedMap<>(entity);
          if (jsonParser.currentToken() == JsonToken.END_OBJECT) {
            yield map;
          }

          var fieldName = jsonParser.currentName();
          jsonParser.nextToken();

          var value = parseValue(db, null, jsonParser, null);
          map.put(fieldName, value);

          yield parseEmbeddedMap(db, entity, jsonParser, map);
        }

        default -> throw new SerializationException("Unexpected value type: " + type);
      };

      default -> throw new SerializationException("Unexpected token: " + token);
    };
  }

  private static LinkMap parseLinkMap(EntityImpl entity, JsonParser jsonParser) throws IOException {
    var map = new LinkMap(entity);
    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      var fieldName = jsonParser.currentName();
      jsonParser.nextToken();
      var value = new RecordId(jsonParser.getText());
      map.put(fieldName, value);
    }
    return map;
  }

  @Nonnull
  private static EmbeddedEntityImpl parseEmbeddedEntity(@Nonnull DatabaseSessionInternal db,
      @Nonnull JsonParser jsonParser, @Nullable RecordMetaData metadata) throws IOException {
    var embedded = (EmbeddedEntityImpl) db.newEmbededEntity();
    if (metadata == null) {
      metadata = parseRecordMetadata(jsonParser, null, EntityImpl.RECORD_TYPE);

      if (metadata == null) {
        metadata = new RecordMetaData(EntityImpl.RECORD_TYPE, null,
            null, Collections.emptyMap());
      }
    }

    parseProperties(db, embedded, metadata, jsonParser);

    return embedded;
  }

  private static TrackedMap<Object> parseEmbeddedMap(@Nonnull DatabaseSessionInternal db,
      @Nonnull EntityImpl entity, @Nonnull JsonParser jsonParser, @Nullable TrackedMap<Object> map)
      throws IOException {
    if (map == null) {
      map = new TrackedMap<>(entity);
    }

    while (jsonParser.nextToken() != JsonToken.END_OBJECT) {
      var fieldName = jsonParser.currentName();
      jsonParser.nextToken();
      var value = parseValue(db, null, jsonParser, null);
      map.put(fieldName, value);
    }

    return map;
  }

  private static LinkList parseLinkList(EntityImpl entity, JsonParser jsonParser)
      throws IOException {
    var list = new LinkList(entity);

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      var ridText = jsonParser.getText();
      list.add(new RecordId(ridText));
    }

    return list;
  }

  private static LinkSet parseLinkSet(EntityImpl entity, JsonParser jsonParser)
      throws IOException {
    var list = new LinkSet(entity);

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      var ridText = jsonParser.getText();
      list.add(new RecordId(ridText));
    }

    return list;
  }

  private static RidBag parseLinkBag(EntityImpl entity, JsonParser jsonParser)
      throws IOException {
    var bag = new RidBag(entity.getSession());

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      var ridText = jsonParser.getText();
      bag.add(new RecordId(ridText));
    }

    return bag;
  }

  private static TrackedList<Object> parseEmbeddedList(DatabaseSessionInternal db,
      EntityImpl entity,
      JsonParser jsonParser) throws IOException {
    var list = new TrackedList<>(entity);

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      list.add(parseValue(db, null, jsonParser, null));
    }

    return list;
  }

  private static TrackedSet<Object> parseEmbeddedSet(DatabaseSessionInternal db, EntityImpl entity,
      JsonParser jsonParser) throws IOException {
    var list = new TrackedSet<>(entity);

    while (jsonParser.nextToken() != JsonToken.END_ARRAY) {
      list.add(parseValue(db, null, jsonParser, null));
    }

    return list;
  }

  @Override
  public String getName() {
    return NAME;
  }

  private record RecordMetaData(byte recordType, RecordId recordId, String className,
                                Map<String, String> fieldTypes) {

  }
}
