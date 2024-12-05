package com.orientechnologies.core.serialization.serializer.record.binary;

import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.MILLISEC_PER_DAY;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.bytesFromString;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.convertDayToTimezone;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.getGlobalProperty;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.getLinkedType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.getTypeFromValueEmbedded;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readBinary;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readByte;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readInteger;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readLinkCollection;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readLinkMap;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readLong;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readOType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readOptimizedLink;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.readString;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.stringFromBytesIntern;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeBinary;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeLinkCollection;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeLinkMap;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeOType;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeOptimizedLink;
import static com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.writeString;

import com.orientechnologies.common.collection.OMultiValue;
import com.orientechnologies.common.serialization.types.OByteSerializer;
import com.orientechnologies.common.serialization.types.ODecimalSerializer;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.common.serialization.types.OLongSerializer;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.LinkList;
import com.orientechnologies.core.db.record.LinkSet;
import com.orientechnologies.core.db.record.RecordElement;
import com.orientechnologies.core.db.record.TrackedList;
import com.orientechnologies.core.db.record.TrackedMap;
import com.orientechnologies.core.db.record.TrackedSet;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.exception.YTValidationException;
import com.orientechnologies.core.metadata.schema.OGlobalProperty;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.security.PropertyEncryption;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.EntityEntry;
import com.orientechnologies.core.record.impl.ODocumentInternal;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTEntityImplEmbedded;
import com.orientechnologies.core.serialization.ODocumentSerializable;
import com.orientechnologies.core.serialization.OSerializableStream;
import com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.MapRecordInfo;
import com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.RecordInfo;
import com.orientechnologies.core.serialization.serializer.record.binary.HelperClasses.Tuple;
import com.orientechnologies.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

public class ORecordSerializerBinaryV1 implements ODocumentSerializer {

  private final OBinaryComparatorV0 comparator = new OBinaryComparatorV0();

  private static int findMatchingFieldName(final BytesContainer bytes, int len, byte[][] fields) {
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i] != null && fields[i].length == len) {
        boolean matchField = true;
        for (int j = 0; j < len; ++j) {
          if (bytes.bytes[bytes.offset + j] != fields[i][j]) {
            matchField = false;
            break;
          }
        }
        if (matchField) {
          return i;
        }
      }
    }

    return -1;
  }

  private static boolean checkIfPropertyNameMatchSome(OGlobalProperty prop, final String[] fields) {
    String fieldName = prop.getName();

    for (String field : fields) {
      if (fieldName.equals(field)) {
        return true;
      }
    }

    return false;
  }

  public void deserializePartial(YTDatabaseSessionInternal db, YTEntityImpl document,
      BytesContainer bytes, String[] iFields) {
    // TRANSFORMS FIELDS FOM STRINGS TO BYTE[]
    final byte[][] fields = new byte[iFields.length][];
    for (int i = 0; i < iFields.length; ++i) {
      fields[i] = bytesFromString(iFields[i]);
    }

    String fieldName;
    YTType type;
    int unmarshalledFields = 0;

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int currentValuePos = valuesStart;

    while (bytes.offset < valuesStart) {

      final int len = OVarIntSerializer.readAsInteger(bytes);
      boolean found;
      int fieldLength;
      if (len > 0) {
        int fieldPos = findMatchingFieldName(bytes, len, fields);
        bytes.skip(len);
        Tuple<Integer, YTType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        fieldLength = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();

        if (fieldPos >= 0) {
          fieldName = iFields[fieldPos];
          found = true;
        } else {
          fieldName = null;
          found = false;
        }
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final OGlobalProperty prop = getGlobalProperty(document, len);
        found = checkIfPropertyNameMatchSome(prop, iFields);

        fieldLength = OVarIntSerializer.readAsInteger(bytes);
        type = getPropertyTypeFromStream(prop, bytes);

        fieldName = prop.getName();
      }
      if (found) {
        if (fieldLength != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = currentValuePos;
          final Object value = deserializeValue(db, bytes, type, document);
          bytes.offset = headerCursor;
          ODocumentInternal.rawField(document, fieldName, value, type);
        } else {
          // If pos us 0 the value is null just set it.
          ODocumentInternal.rawField(document, fieldName, null, null);
        }
        if (++unmarshalledFields == iFields.length)
        // ALL REQUESTED FIELDS UNMARSHALLED: EXIT
        {
          break;
        }
      }
      currentValuePos += fieldLength;
    }
  }

  private boolean checkMatchForLargerThenZero(
      final BytesContainer bytes, final byte[] field, int len) {
    if (field.length != len) {
      return false;
    }
    boolean match = true;
    for (int j = 0; j < len; ++j) {
      if (bytes.bytes[bytes.offset + j] != field[j]) {
        match = false;
        break;
      }
    }

    return match;
  }

  private YTType getPropertyTypeFromStream(final OGlobalProperty prop, final BytesContainer bytes) {
    final YTType type;
    if (prop.getType() != YTType.ANY) {
      type = prop.getType();
    } else {
      type = readOType(bytes, false);
    }

    return type;
  }

  public OBinaryField deserializeField(
      final BytesContainer bytes,
      final YTClass iClass,
      final String iFieldName,
      boolean embedded,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {

    if (embedded) {
      // skip class name bytes
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int currentValuePos = valuesStart;

    while (bytes.offset < valuesStart) {

      final int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {

        boolean match = checkMatchForLargerThenZero(bytes, field, len);

        bytes.skip(len);
        Tuple<Integer, YTType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        final int fieldLength = pointerAndType.getFirstVal();
        final YTType type = pointerAndType.getSecondVal();

        if (match) {
          if (fieldLength == 0 || !getComparator().isBinaryComparable(type)) {
            return null;
          }

          bytes.offset = currentValuePos;
          return new OBinaryField(iFieldName, type, bytes, null);
        }
        currentValuePos += fieldLength;
      } else {

        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
        final YTType type;
        type = getPropertyTypeFromStream(prop, bytes);

        if (iFieldName.equals(prop.getName())) {
          if (fieldLength == 0 || !getComparator().isBinaryComparable(type)) {
            return null;
          }
          bytes.offset = currentValuePos;
          final YTProperty classProp = iClass.getProperty(iFieldName);
          return new OBinaryField(
              iFieldName, type, bytes, classProp != null ? classProp.getCollate() : null);
        }
        currentValuePos += fieldLength;
      }
    }
    return null;
  }

  public void deserialize(YTDatabaseSessionInternal db, final YTEntityImpl document,
      final BytesContainer bytes) {
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int last = 0;
    String fieldName;
    YTType type;
    int cumulativeSize = valuesStart;
    while (bytes.offset < valuesStart) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      int fieldLength;
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
        bytes.skip(len);
        Tuple<Integer, YTType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        fieldLength = pointerAndType.getFirstVal();
        type = pointerAndType.getSecondVal();
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        prop = getGlobalProperty(document, len);
        fieldName = prop.getName();
        fieldLength = OVarIntSerializer.readAsInteger(bytes);
        type = getPropertyTypeFromStream(prop, bytes);
      }

      if (!ODocumentInternal.rawContainsField(document, fieldName)) {
        if (fieldLength != 0) {
          int headerCursor = bytes.offset;

          bytes.offset = cumulativeSize;
          final Object value = deserializeValue(db, bytes, type, document);
          if (bytes.offset > last) {
            last = bytes.offset;
          }
          bytes.offset = headerCursor;
          ODocumentInternal.rawField(document, fieldName, value, type);
        } else {
          ODocumentInternal.rawField(document, fieldName, null, null);
        }
      }

      cumulativeSize += fieldLength;
    }

    ORecordInternal.clearSource(document);

    if (last > bytes.offset) {
      bytes.offset = last;
    }
  }

  public void deserializeWithClassName(YTDatabaseSessionInternal db, final YTEntityImpl document,
      final BytesContainer bytes) {

    final String className = readString(bytes);
    if (!className.isEmpty()) {
      ODocumentInternal.fillClassNameIfNeeded(document, className);
    }

    deserialize(db, document, bytes);
  }

  public String[] getFieldNames(YTEntityImpl reference, final BytesContainer bytes,
      boolean embedded) {
    // SKIP CLASS NAME
    if (embedded) {
      final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
      bytes.skip(classNameLen);
    }

    // skip header length
    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;

    final List<String> result = new ArrayList<>();

    String fieldName;
    while (bytes.offset < headerStart + headerLength) {
      OGlobalProperty prop;
      final int len = OVarIntSerializer.readAsInteger(bytes);
      if (len > 0) {
        // PARSE FIELD NAME
        fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
        result.add(fieldName);

        // SKIP THE REST
        bytes.skip(len);
        OVarIntSerializer.readAsInteger(bytes);
        bytes.skip(1);
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        prop = ODocumentInternal.getGlobalPropertyById(reference, id);
        if (prop == null) {
          throw new YTSerializationException(
              "Missing property definition for property id '" + id + "'");
        }
        result.add(prop.getName());

        // SKIP THE REST
        OVarIntSerializer.readAsInteger(bytes);
        if (prop.getType() == YTType.ANY) {
          bytes.skip(1);
        }
      }
    }

    return result.toArray(new String[0]);
  }

  private void serializeValues(
      YTDatabaseSessionInternal session, final BytesContainer headerBuffer,
      final BytesContainer valuesBuffer,
      final YTEntityImpl document,
      Set<Entry<String, EntityEntry>> fields,
      final Map<String, YTProperty> props,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    YTClass oClass = ODocumentInternal.getImmutableSchemaClass(document);
    for (Entry<String, EntityEntry> field : fields) {
      EntityEntry docEntry = field.getValue();
      if (!field.getValue().exists()) {
        continue;
      }
      if (docEntry.property == null && props != null) {
        YTProperty prop = props.get(field.getKey());
        if (prop != null && docEntry.type == prop.getType()) {
          docEntry.property = prop;
        }
      }

      if (docEntry.property == null) {
        String fieldName = field.getKey();
        writeString(headerBuffer, fieldName);
      } else {
        OVarIntSerializer.write(headerBuffer, (docEntry.property.getId() + 1) * -1);
      }

      final Object value = field.getValue().value;

      final YTType type;
      if (value != null) {
        type = getFieldType(field.getValue());
        if (type == null) {
          throw new YTSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the YTEntityImpl binary serializer");
        }
        int startOffset = valuesBuffer.offset;
        serializeValue(session,
            valuesBuffer,
            value,
            type,
            getLinkedType(oClass, type, field.getKey()),
            schema, encryption);
        int valueLength = valuesBuffer.offset - startOffset;
        OVarIntSerializer.write(headerBuffer, valueLength);
      } else {
        // handle null fields
        OVarIntSerializer.write(headerBuffer, 0);
        type = YTType.ANY;
      }

      // write type. Type should be written both for regular and null fields
      if (field.getValue().property == null || field.getValue().property.getType() == YTType.ANY) {
        int typeOffset = headerBuffer.alloc(OByteSerializer.BYTE_SIZE);
        OByteSerializer.INSTANCE.serialize((byte) type.getId(), headerBuffer.bytes, typeOffset);
      }
    }
  }

  private void merge(
      BytesContainer destinationBuffer,
      BytesContainer sourceBuffer1,
      BytesContainer sourceBuffer2) {
    destinationBuffer.offset =
        destinationBuffer.allocExact(sourceBuffer1.offset + sourceBuffer2.offset);
    System.arraycopy(
        sourceBuffer1.bytes,
        0,
        destinationBuffer.bytes,
        destinationBuffer.offset,
        sourceBuffer1.offset);
    System.arraycopy(
        sourceBuffer2.bytes,
        0,
        destinationBuffer.bytes,
        destinationBuffer.offset + sourceBuffer1.offset,
        sourceBuffer2.offset);
    destinationBuffer.offset += sourceBuffer1.offset + sourceBuffer2.offset;
  }

  private void serializeDocument(
      YTDatabaseSessionInternal session, final YTEntityImpl document,
      final BytesContainer bytes,
      final YTClass clazz,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    // allocate space for header length

    final Map<String, YTProperty> props = clazz != null ? clazz.propertiesMap(session) : null;
    final Set<Entry<String, EntityEntry>> fields = ODocumentInternal.rawEntries(document);

    BytesContainer valuesBuffer = new BytesContainer();
    BytesContainer headerBuffer = new BytesContainer();

    serializeValues(session, headerBuffer, valuesBuffer, document, fields, props, schema,
        encryption);
    int headerLength = headerBuffer.offset;
    // write header length as soon as possible
    OVarIntSerializer.write(bytes, headerLength);

    merge(bytes, headerBuffer, valuesBuffer);
  }

  public void serializeWithClassName(YTDatabaseSessionInternal session, final YTEntityImpl document,
      final BytesContainer bytes) {
    YTImmutableSchema schema = ODocumentInternal.getImmutableSchema(document);
    final YTClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    if (clazz != null && document.isEmbedded()) {
      writeString(bytes, clazz.getName());
    } else {
      writeEmptyString(bytes);
    }
    PropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(document);
    serializeDocument(session, document, bytes, clazz, schema, encryption);
  }

  public void serialize(YTDatabaseSessionInternal session, final YTEntityImpl document,
      final BytesContainer bytes) {
    YTImmutableSchema schema = ODocumentInternal.getImmutableSchema(document);
    PropertyEncryption encryption = ODocumentInternal.getPropertyEncryption(document);
    final YTClass clazz = ODocumentInternal.getImmutableSchemaClass(document);
    serializeDocument(session, document, bytes, clazz, schema, encryption);
  }

  public boolean isSerializingClassNameByDefault() {
    return false;
  }

  public <RET> RET deserializeFieldTyped(
      YTDatabaseSessionInternal session, BytesContainer bytes,
      String iFieldName,
      boolean isEmbedded,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    if (isEmbedded) {
      skipClassName(bytes);
    }
    return deserializeFieldTypedLoopAndReturn(session, bytes, iFieldName, schema, encryption);
  }

  protected <RET> RET deserializeFieldTypedLoopAndReturn(
      YTDatabaseSessionInternal session, BytesContainer bytes,
      String iFieldName,
      final YTImmutableSchema schema,
      PropertyEncryption encryption) {
    final byte[] field = iFieldName.getBytes();

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerStart = bytes.offset;
    int valuesStart = headerStart + headerLength;
    int cumulativeLength = valuesStart;

    while (bytes.offset < valuesStart) {

      int len = OVarIntSerializer.readAsInteger(bytes);

      if (len > 0) {
        // CHECK BY FIELD NAME SIZE: THIS AVOID EVEN THE UNMARSHALLING OF FIELD NAME
        boolean match = checkMatchForLargerThenZero(bytes, field, len);

        bytes.skip(len);
        Tuple<Integer, YTType> pointerAndType = getFieldSizeAndTypeFromCurrentPosition(bytes);
        int fieldLength = pointerAndType.getFirstVal();
        YTType type = pointerAndType.getSecondVal();

        if (match) {
          if (fieldLength == 0) {
            return null;
          }

          bytes.offset = cumulativeLength;
          Object value = deserializeValue(session, bytes, type, null, false, fieldLength, false,
              schema);
          //noinspection unchecked
          return (RET) value;
        }
        cumulativeLength += fieldLength;
      } else {
        // LOAD GLOBAL PROPERTY BY ID
        final int id = (len * -1) - 1;
        final OGlobalProperty prop = schema.getGlobalPropertyById(id);
        final int fieldLength = OVarIntSerializer.readAsInteger(bytes);
        YTType type = getPropertyTypeFromStream(prop, bytes);

        if (iFieldName.equals(prop.getName())) {

          if (fieldLength == 0) {
            return null;
          }

          bytes.offset = cumulativeLength;

          Object value = deserializeValue(session, bytes, type, null,
              false, fieldLength, false,
              schema);
          //noinspection unchecked
          return (RET) value;
        }
        cumulativeLength += fieldLength;
      }
    }
    return null;
  }

  /**
   * use only for named fields
   */
  private Tuple<Integer, YTType> getFieldSizeAndTypeFromCurrentPosition(BytesContainer bytes) {
    int fieldSize = OVarIntSerializer.readAsInteger(bytes);
    YTType type = readOType(bytes, false);
    return new Tuple<>(fieldSize, type);
  }

  public void deserializeDebug(
      YTDatabaseSessionInternal db, BytesContainer bytes,
      ORecordSerializationDebug debugInfo,
      YTImmutableSchema schema) {

    int headerLength = OVarIntSerializer.readAsInteger(bytes);
    int headerPos = bytes.offset;

    debugInfo.properties = new ArrayList<>();
    int last = 0;
    String fieldName;
    YTType type;
    int cumulativeLength = 0;
    while (true) {
      ORecordSerializationDebugProperty debugProperty = new ORecordSerializationDebugProperty();
      OGlobalProperty prop;

      int fieldLength;

      try {
        if (bytes.offset >= headerPos + headerLength) {
          break;
        }

        final int len = OVarIntSerializer.readAsInteger(bytes);
        debugInfo.properties.add(debugProperty);
        if (len > 0) {
          // PARSE FIELD NAME
          fieldName = stringFromBytesIntern(bytes.bytes, bytes.offset, len);
          bytes.skip(len);

          Tuple<Integer, YTType> valuePositionAndType =
              getFieldSizeAndTypeFromCurrentPosition(bytes);
          fieldLength = valuePositionAndType.getFirstVal();
          type = valuePositionAndType.getSecondVal();
        } else {
          // LOAD GLOBAL PROPERTY BY ID
          final int id = (len * -1) - 1;
          debugProperty.globalId = id;
          prop = schema.getGlobalPropertyById(id);
          fieldLength = OVarIntSerializer.readAsInteger(bytes);
          debugProperty.valuePos = headerPos + headerLength + cumulativeLength;
          if (prop != null) {
            fieldName = prop.getName();
            type = getPropertyTypeFromStream(prop, bytes);
          } else {
            cumulativeLength += fieldLength;
            continue;
          }
        }
        debugProperty.name = fieldName;
        debugProperty.type = type;

        int valuePos;
        if (fieldLength > 0) {
          valuePos = headerPos + headerLength + cumulativeLength;
        } else {
          valuePos = 0;
        }

        cumulativeLength += fieldLength;

        if (valuePos != 0) {
          int headerCursor = bytes.offset;
          bytes.offset = valuePos;
          try {
            debugProperty.value = deserializeValue(db, bytes, type, new YTEntityImpl());
          } catch (RuntimeException ex) {
            debugProperty.faildToRead = true;
            debugProperty.readingException = ex;
            debugProperty.failPosition = bytes.offset;
          }
          if (bytes.offset > last) {
            last = bytes.offset;
          }
          bytes.offset = headerCursor;
        } else {
          debugProperty.value = null;
        }
      } catch (RuntimeException ex) {
        debugInfo.readingFailure = true;
        debugInfo.readingException = ex;
        debugInfo.failPosition = bytes.offset;
      }
    }
  }

  @SuppressWarnings("unchecked")
  protected int writeEmbeddedMap(
      YTDatabaseSessionInternal session, BytesContainer bytes,
      Map<Object, Object> map,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    final int fullPos = OVarIntSerializer.write(bytes, map.size());
    for (Entry<Object, Object> entry : map.entrySet()) {
      // TODO:check skip of complex types
      // FIXME: changed to support only string key on map
      YTType type = YTType.STRING;
      writeOType(bytes, bytes.alloc(1), type);
      var key = entry.getKey();
      if (key == null) {
        throw new YTSerializationException("Maps with null keys are not supported");
      }
      writeString(bytes, entry.getKey().toString());
      final Object value = entry.getValue();
      if (value != null) {
        type = getTypeFromValueEmbedded(value);
        if (type == null) {
          throw new YTSerializationException(
              "Impossible serialize value of type "
                  + value.getClass()
                  + " with the YTEntityImpl binary serializer");
        }
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(session, bytes, value, type, null, schema, encryption);
      } else {
        // signal for null value
        int pointer = bytes.alloc(1);
        OByteSerializer.INSTANCE.serialize((byte) -1, bytes.bytes, pointer);
      }
    }

    return fullPos;
  }

  protected Object readEmbeddedMap(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {
    int size = OVarIntSerializer.readAsInteger(bytes);
    final TrackedMap<Object> result = new TrackedMap<>(owner);
    for (int i = 0; i < size; i++) {
      YTType keyType = readOType(bytes, false);
      Object key = deserializeValue(db, bytes, keyType, result);
      final YTType type = HelperClasses.readType(bytes);
      if (type != null) {
        Object value = deserializeValue(db, bytes, type, result);
        result.putInternal(key, value);
      } else {
        result.putInternal(key, null);
      }
    }
    return result;
  }

  protected List<MapRecordInfo> getPositionsFromEmbeddedMap(
      YTDatabaseSessionInternal session, final BytesContainer bytes, YTImmutableSchema schema) {
    List<MapRecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      YTType keyType = readOType(bytes, false);
      String key = readString(bytes);
      YTType valueType = HelperClasses.readType(bytes);
      MapRecordInfo recordInfo = new MapRecordInfo();
      recordInfo.fieldType = valueType;
      recordInfo.key = key;
      recordInfo.keyType = keyType;
      int currentOffset = bytes.offset;

      if (valueType != null) {
        recordInfo.fieldStartOffset = bytes.offset;
        deserializeValue(session, bytes, valueType, null, true,
            -1, true, schema);
        recordInfo.fieldLength = bytes.offset - currentOffset;
        retList.add(recordInfo);
      } else {
        recordInfo.fieldStartOffset = 0;
        recordInfo.fieldLength = 0;
        retList.add(recordInfo);
      }
    }

    return retList;
  }

  protected int writeRidBag(BytesContainer bytes, RidBag ridbag) {
    int positionOffset = bytes.offset;
    HelperClasses.writeRidBag(bytes, ridbag);
    return positionOffset;
  }

  protected RidBag readRidbag(YTDatabaseSessionInternal session, BytesContainer bytes) {
    return HelperClasses.readRidbag(session, bytes);
  }

  public Object deserializeValue(
      YTDatabaseSessionInternal session, final BytesContainer bytes, final YTType type,
      final RecordElement owner) {
    RecordElement doc = owner;
    while (!(doc instanceof YTEntityImpl) && doc != null) {
      doc = doc.getOwner();
    }
    YTImmutableSchema schema = ODocumentInternal.getImmutableSchema((YTEntityImpl) doc);
    return deserializeValue(session, bytes, type, owner, true, -1, false, schema);
  }

  protected Object deserializeValue(
      YTDatabaseSessionInternal session, final BytesContainer bytes,
      final YTType type,
      final RecordElement owner,
      boolean embeddedAsDocument,
      int valueLengthInBytes,
      boolean justRunThrough,
      YTImmutableSchema schema) {
    if (type == null) {
      throw new YTDatabaseException("Invalid type value: null");
    }
    Object value = null;
    switch (type) {
      case INTEGER:
        value = OVarIntSerializer.readAsInteger(bytes);
        break;
      case LONG:
        value = OVarIntSerializer.readAsLong(bytes);
        break;
      case SHORT:
        value = OVarIntSerializer.readAsShort(bytes);
        break;
      case STRING:
        if (justRunThrough) {
          int length = OVarIntSerializer.readAsInteger(bytes);
          bytes.skip(length);
        } else {
          value = readString(bytes);
        }
        break;
      case DOUBLE:
        if (justRunThrough) {
          bytes.skip(OLongSerializer.LONG_SIZE);
        } else {
          value = Double.longBitsToDouble(readLong(bytes));
        }
        break;
      case FLOAT:
        if (justRunThrough) {
          bytes.skip(OIntegerSerializer.INT_SIZE);
        } else {
          value = Float.intBitsToFloat(readInteger(bytes));
        }
        break;
      case BYTE:
        if (justRunThrough) {
          bytes.offset++;
        } else {
          value = readByte(bytes);
        }
        break;
      case BOOLEAN:
        if (justRunThrough) {
          bytes.offset++;
        } else {
          value = readByte(bytes) == 1;
        }
        break;
      case DATETIME:
        if (justRunThrough) {
          OVarIntSerializer.readAsLong(bytes);
        } else {
          value = new Date(OVarIntSerializer.readAsLong(bytes));
        }
        break;
      case DATE:
        if (justRunThrough) {
          OVarIntSerializer.readAsLong(bytes);
        } else {
          long savedTime = OVarIntSerializer.readAsLong(bytes) * MILLISEC_PER_DAY;
          savedTime =
              convertDayToTimezone(
                  TimeZone.getTimeZone("GMT"), ODateHelper.getDatabaseTimeZone(), savedTime);
          value = new Date(savedTime);
        }
        break;
      case EMBEDDED:
        if (embeddedAsDocument) {
          value = deserializeEmbeddedAsDocument(session, bytes, owner);
        } else {
          value = deserializeEmbeddedAsBytes(session, bytes, valueLengthInBytes, schema);
        }
        break;
      case EMBEDDEDSET:
        if (embeddedAsDocument) {
          value = readEmbeddedSet(session, bytes, owner);
        } else {
          value = deserializeEmbeddedCollectionAsCollectionOfBytes(session, bytes, schema);
        }
        break;
      case EMBEDDEDLIST:
        if (embeddedAsDocument) {
          value = readEmbeddedList(session, bytes, owner);
        } else {
          value = deserializeEmbeddedCollectionAsCollectionOfBytes(session, bytes, schema);
        }
        break;
      case LINKSET:
        LinkSet collectionSet = null;
        if (!justRunThrough) {
          collectionSet = new LinkSet(owner);
        }
        value = readLinkCollection(bytes, collectionSet, justRunThrough);
        break;
      case LINKLIST:
        LinkList collectionList = null;
        if (!justRunThrough) {
          collectionList = new LinkList(owner);
        }
        value = readLinkCollection(bytes, collectionList, justRunThrough);
        break;
      case BINARY:
        if (justRunThrough) {
          int len = OVarIntSerializer.readAsInteger(bytes);
          bytes.skip(len);
        } else {
          value = readBinary(bytes);
        }
        break;
      case LINK:
        value = readOptimizedLink(bytes, justRunThrough);
        break;
      case LINKMAP:
        value = readLinkMap(bytes, owner, justRunThrough);
        break;
      case EMBEDDEDMAP:
        if (embeddedAsDocument) {
          value = readEmbeddedMap(session, bytes, owner);
        } else {
          value = deserializeEmbeddedMapAsMapOfBytes(session, bytes, schema);
        }
        break;
      case DECIMAL:
        value = ODecimalSerializer.INSTANCE.deserialize(bytes.bytes, bytes.offset);
        bytes.skip(ODecimalSerializer.INSTANCE.getObjectSize(bytes.bytes, bytes.offset));
        break;
      case LINKBAG:
        RidBag bag = readRidbag(session, bytes);
        bag.setOwner(owner);
        value = bag;
        break;
      case TRANSIENT:
        break;
      case CUSTOM:
        try {
          String className = readString(bytes);
          Class<?> clazz = Class.forName(className);
          OSerializableStream stream = (OSerializableStream) clazz.newInstance();
          byte[] bytesRepresentation = readBinary(bytes);
          if (embeddedAsDocument) {
            stream.fromStream(bytesRepresentation);
            if (stream instanceof OSerializableWrapper) {
              value = ((OSerializableWrapper) stream).getSerializable();
            } else {
              value = stream;
            }
          } else {
            YTResultBinary retVal =
                new YTResultBinary(session, schema, bytesRepresentation, 0,
                    bytesRepresentation.length, this);
            return retVal;
          }
        } catch (Exception e) {
          throw new RuntimeException(e);
        }
        break;
      case ANY:
        break;
    }
    return value;
  }

  protected YTType getFieldType(final EntityEntry entry) {
    YTType type = entry.type;
    if (type == null) {
      final YTProperty prop = entry.property;
      if (prop != null) {
        type = prop.getType();
      }
    }
    if (type == null || YTType.ANY == type) {
      type = YTType.getTypeByValue(entry.value);
    }
    return type;
  }

  protected int writeEmptyString(final BytesContainer bytes) {
    return OVarIntSerializer.write(bytes, 0);
  }

  @SuppressWarnings("unchecked")
  @Override
  public int serializeValue(
      YTDatabaseSessionInternal session, final BytesContainer bytes,
      Object value,
      final YTType type,
      final YTType linkedType,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    int pointer = 0;
    switch (type) {
      case INTEGER:
      case LONG:
      case SHORT:
        pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
        break;
      case STRING:
        pointer = writeString(bytes, value.toString());
        break;
      case DOUBLE:
        long dg = Double.doubleToLongBits(((Number) value).doubleValue());
        pointer = bytes.alloc(OLongSerializer.LONG_SIZE);
        OLongSerializer.INSTANCE.serializeLiteral(dg, bytes.bytes, pointer);
        break;
      case FLOAT:
        int fg = Float.floatToIntBits(((Number) value).floatValue());
        pointer = bytes.alloc(OIntegerSerializer.INT_SIZE);
        OIntegerSerializer.INSTANCE.serializeLiteral(fg, bytes.bytes, pointer);
        break;
      case BYTE:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = ((Number) value).byteValue();
        break;
      case BOOLEAN:
        pointer = bytes.alloc(1);
        bytes.bytes[pointer] = ((Boolean) value) ? (byte) 1 : (byte) 0;
        break;
      case DATETIME:
        if (value instanceof Number) {
          pointer = OVarIntSerializer.write(bytes, ((Number) value).longValue());
        } else {
          pointer = OVarIntSerializer.write(bytes, ((Date) value).getTime());
        }
        break;
      case DATE:
        long dateValue;
        if (value instanceof Number) {
          dateValue = ((Number) value).longValue();
        } else {
          dateValue = ((Date) value).getTime();
        }
        dateValue =
            convertDayToTimezone(
                ODateHelper.getDatabaseTimeZone(), TimeZone.getTimeZone("GMT"), dateValue);
        pointer = OVarIntSerializer.write(bytes, dateValue / MILLISEC_PER_DAY);
        break;
      case EMBEDDED:
        pointer = bytes.offset;
        if (value instanceof ODocumentSerializable) {
          YTEntityImpl cur = ((ODocumentSerializable) value).toDocument();
          cur.field(ODocumentSerializable.CLASS_NAME, value.getClass().getName());
          serializeWithClassName(session, cur, bytes);
        } else {
          serializeWithClassName(session, (YTEntityImpl) value, bytes);
        }
        break;
      case EMBEDDEDSET:
      case EMBEDDEDLIST:
        if (value.getClass().isArray()) {
          pointer =
              writeEmbeddedCollection(session,
                  bytes, Arrays.asList(OMultiValue.array(value)), linkedType, schema, encryption);
        } else {
          pointer =
              writeEmbeddedCollection(session, bytes, (Collection<?>) value, linkedType, schema,
                  encryption);
        }
        break;
      case DECIMAL:
        BigDecimal decimalValue = (BigDecimal) value;
        pointer = bytes.alloc(ODecimalSerializer.INSTANCE.getObjectSize(decimalValue));
        ODecimalSerializer.INSTANCE.serialize(decimalValue, bytes.bytes, pointer);
        break;
      case BINARY:
        pointer = writeBinary(bytes, (byte[]) (value));
        break;
      case LINKSET:
      case LINKLIST:
        Collection<YTIdentifiable> ridCollection = (Collection<YTIdentifiable>) value;
        pointer = writeLinkCollection(bytes, ridCollection);
        break;
      case LINK:
        if (!(value instanceof YTIdentifiable)) {
          throw new YTValidationException("Value '" + value + "' is not a YTIdentifiable");
        }

        pointer = writeOptimizedLink(bytes, (YTIdentifiable) value);
        break;
      case LINKMAP:
        pointer = writeLinkMap(bytes, (Map<Object, YTIdentifiable>) value);
        break;
      case EMBEDDEDMAP:
        pointer = writeEmbeddedMap(session, bytes, (Map<Object, Object>) value, schema, encryption);
        break;
      case LINKBAG:
        pointer = writeRidBag(bytes, (RidBag) value);
        break;
      case CUSTOM:
        if (!(value instanceof OSerializableStream)) {
          value = new OSerializableWrapper((Serializable) value);
        }
        pointer = writeString(bytes, value.getClass().getName());
        writeBinary(bytes, ((OSerializableStream) value).toStream());
        break;
      case TRANSIENT:
        break;
      case ANY:
        break;
    }
    return pointer;
  }

  protected int writeEmbeddedCollection(
      YTDatabaseSessionInternal session, final BytesContainer bytes,
      final Collection<?> value,
      final YTType linkedType,
      YTImmutableSchema schema,
      PropertyEncryption encryption) {
    final int pos = OVarIntSerializer.write(bytes, value.size());
    // TODO manage embedded type from schema and auto-determined.
    writeOType(bytes, bytes.alloc(1), YTType.ANY);
    for (Object itemValue : value) {
      // TODO:manage in a better way null entry
      if (itemValue == null) {
        writeOType(bytes, bytes.alloc(1), YTType.ANY);
        continue;
      }
      YTType type;
      if (linkedType == null || linkedType == YTType.ANY) {
        type = getTypeFromValueEmbedded(itemValue);
      } else {
        type = linkedType;
      }
      if (type != null) {
        writeOType(bytes, bytes.alloc(1), type);
        serializeValue(session, bytes, itemValue, type, null, schema, encryption);
      } else {
        throw new YTSerializationException(
            "Impossible serialize value of type "
                + value.getClass()
                + " with the YTEntityImpl binary serializer");
      }
    }
    return pos;
  }

  protected List deserializeEmbeddedCollectionAsCollectionOfBytes(
      YTDatabaseSessionInternal db, final BytesContainer bytes, YTImmutableSchema schema) {
    List retVal = new ArrayList();
    List<RecordInfo> fieldsInfo = getPositionsFromEmbeddedCollection(db, bytes, schema);
    for (RecordInfo fieldInfo : fieldsInfo) {
      if (fieldInfo.fieldType.isEmbedded()) {
        YTResultBinary result =
            new YTResultBinary(db,
                schema, bytes.bytes, fieldInfo.fieldStartOffset, fieldInfo.fieldLength, this);
        retVal.add(result);
      } else {
        int currentOffset = bytes.offset;
        bytes.offset = fieldInfo.fieldStartOffset;
        Object value = deserializeValue(db, bytes, fieldInfo.fieldType, null);
        retVal.add(value);
        bytes.offset = currentOffset;
      }
    }

    return retVal;
  }

  protected Map<String, Object> deserializeEmbeddedMapAsMapOfBytes(
      YTDatabaseSessionInternal db, final BytesContainer bytes, YTImmutableSchema schema) {
    Map<String, Object> retVal = new TreeMap<>();
    List<MapRecordInfo> positionsWithLengths = getPositionsFromEmbeddedMap(db, bytes, schema);
    for (MapRecordInfo recordInfo : positionsWithLengths) {
      String key = recordInfo.key;
      Object value;
      if (recordInfo.fieldType != null && recordInfo.fieldType.isEmbedded()) {
        value =
            new YTResultBinary(db,
                schema, bytes.bytes, recordInfo.fieldStartOffset, recordInfo.fieldLength, this);
      } else if (recordInfo.fieldStartOffset != 0) {
        int currentOffset = bytes.offset;
        bytes.offset = recordInfo.fieldStartOffset;
        value = deserializeValue(db, bytes, recordInfo.fieldType, null);
        bytes.offset = currentOffset;
      } else {
        value = null;
      }
      retVal.put(key, value);
    }
    return retVal;
  }

  protected Object deserializeEmbeddedAsDocument(
      YTDatabaseSessionInternal db, final BytesContainer bytes, final RecordElement owner) {
    Object value = new YTEntityImplEmbedded();
    deserializeWithClassName(db, (YTEntityImpl) value, bytes);
    if (((YTEntityImpl) value).containsField(ODocumentSerializable.CLASS_NAME)) {
      String className = ((YTEntityImpl) value).field(ODocumentSerializable.CLASS_NAME);
      try {
        Class<?> clazz = Class.forName(className);
        ODocumentSerializable newValue = (ODocumentSerializable) clazz.newInstance();
        newValue.fromDocument((YTEntityImpl) value);
        value = newValue;
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    } else {
      var doc = (YTEntityImpl) value;
      ODocumentInternal.addOwner(doc, owner);
      ORecordInternal.unsetDirty(doc);
    }

    return value;
  }

  // returns begin position and length for each value in embedded collection
  private List<RecordInfo> getPositionsFromEmbeddedCollection(
      YTDatabaseSessionInternal session, final BytesContainer bytes, YTImmutableSchema schema) {
    List<RecordInfo> retList = new ArrayList<>();

    int numberOfElements = OVarIntSerializer.readAsInteger(bytes);
    // read collection type
    readByte(bytes);

    for (int i = 0; i < numberOfElements; i++) {
      // read element

      // read data type
      YTType dataType = readOType(bytes, false);
      int fieldStart = bytes.offset;

      RecordInfo fieldInfo = new RecordInfo();
      fieldInfo.fieldStartOffset = fieldStart;
      fieldInfo.fieldType = dataType;

      // TODO find better way to skip data bytes;
      deserializeValue(session, bytes, dataType, null, true,
          -1, true, schema);
      fieldInfo.fieldLength = bytes.offset - fieldStart;
      retList.add(fieldInfo);
    }

    return retList;
  }

  protected YTResultBinary deserializeEmbeddedAsBytes(
      YTDatabaseSessionInternal db, final BytesContainer bytes, int valueLength,
      YTImmutableSchema schema) {
    int startOffset = bytes.offset;
    return new YTResultBinary(db, schema, bytes.bytes, startOffset, valueLength, this);
  }

  protected Collection<?> readEmbeddedSet(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    YTType type = readOType(bytes, false);

    if (type == YTType.ANY) {
      final TrackedSet found = new TrackedSet<>(owner);
      for (int i = 0; i < items; i++) {
        YTType itemType = readOType(bytes, false);
        if (itemType == YTType.ANY) {
          found.addInternal(null);
        } else {
          found.addInternal(deserializeValue(db, bytes, itemType, found));
        }
      }
      return found;
    }
    // TODO: manage case where type is known
    return null;
  }

  protected Collection<?> readEmbeddedList(YTDatabaseSessionInternal db, final BytesContainer bytes,
      final RecordElement owner) {

    final int items = OVarIntSerializer.readAsInteger(bytes);
    YTType type = readOType(bytes, false);

    if (type == YTType.ANY) {
      final TrackedList found = new TrackedList<>(owner);
      for (int i = 0; i < items; i++) {
        YTType itemType = readOType(bytes, false);
        if (itemType == YTType.ANY) {
          found.addInternal(null);
        } else {
          found.addInternal(deserializeValue(db, bytes, itemType, found));
        }
      }
      return found;
    }
    // TODO: manage case where type is known
    return null;
  }

  protected void skipClassName(BytesContainer bytes) {
    final int classNameLen = OVarIntSerializer.readAsInteger(bytes);
    bytes.skip(classNameLen);
  }

  @Override
  public OBinaryComparator getComparator() {
    return comparator;
  }
}
