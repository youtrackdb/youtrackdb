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
package com.orientechnologies.orient.core.serialization.serializer.record.string;

import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.core.serialization.serializer.record.ORecordSerializer;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerAnyStreamable;
import com.orientechnologies.orient.core.serialization.serializer.string.OStringSerializerEmbedded;
import com.orientechnologies.orient.core.util.ODateHelper;
import java.io.Serializable;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Calendar;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

@SuppressWarnings("serial")
public abstract class ORecordSerializerStringAbstract implements ORecordSerializer, Serializable {

  protected static final OProfiler PROFILER = YouTrackDBManager.instance().getProfiler();
  private static final char DECIMAL_SEPARATOR = '.';
  private static final String MAX_INTEGER_AS_STRING = String.valueOf(Integer.MAX_VALUE);
  private static final int MAX_INTEGER_DIGITS = MAX_INTEGER_AS_STRING.length();

  public static Object fieldTypeFromStream(
      YTDatabaseSessionInternal db, final YTEntityImpl iDocument, YTType iType,
      final Object iValue) {
    if (iValue == null) {
      return null;
    }

    if (iType == null) {
      iType = YTType.EMBEDDED;
    }

    switch (iType) {
      case STRING:
      case INTEGER:
      case BOOLEAN:
      case FLOAT:
      case DECIMAL:
      case LONG:
      case DOUBLE:
      case SHORT:
      case BYTE:
      case BINARY:
      case DATE:
      case DATETIME:
      case LINK:
        return simpleValueFromStream(db, iValue, iType);

      case EMBEDDED: {
        // EMBEDED RECORD
        final Object embeddedObject =
            OStringSerializerEmbedded.INSTANCE.fromStream(db, (String) iValue);
        if (embeddedObject instanceof YTEntityImpl) {
          ODocumentInternal.addOwner((YTEntityImpl) embeddedObject, iDocument);
        }

        // EMBEDDED OBJECT
        return embeddedObject;
      }

      case CUSTOM:
        // RECORD
        final Object result = OStringSerializerAnyStreamable.INSTANCE.fromStream(db,
            (String) iValue);
        if (result instanceof YTEntityImpl) {
          ODocumentInternal.addOwner((YTEntityImpl) result, iDocument);
        }
        return result;

      case EMBEDDEDSET:
      case EMBEDDEDLIST: {
        final String value = (String) iValue;
        return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionFromStream(db,
            iDocument, iType, null, null, value);
      }

      case EMBEDDEDMAP: {
        final String value = (String) iValue;
        return ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapFromStream(db,
            iDocument, null, value, null);
      }
    }

    throw new IllegalArgumentException(
        "Type " + iType + " not supported to convert value: " + iValue);
  }

  public static Object convertValue(YTDatabaseSessionInternal db,
      final String iValue, final YTType iExpectedType) {
    final Object v = getTypeValue(db, iValue);
    return YTType.convert(db, v, iExpectedType.getDefaultJavaType());
  }

  public static void fieldTypeToString(
      final StringBuilder iBuffer, YTType iType, final Object iValue) {
    if (iValue == null) {
      return;
    }

    final long timer = PROFILER.startChrono();

    if (iType == null) {
      if (iValue instanceof YTRID) {
        iType = YTType.LINK;
      } else {
        iType = YTType.EMBEDDED;
      }
    }

    switch (iType) {
      case STRING:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.string2string"),
            "Serialize string to string",
            timer);
        break;

      case BOOLEAN:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.bool2string"),
            "Serialize boolean to string",
            timer);
        break;

      case INTEGER:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.int2string"),
            "Serialize integer to string",
            timer);
        break;

      case FLOAT:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.float2string"),
            "Serialize float to string",
            timer);
        break;

      case DECIMAL:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.decimal2string"),
            "Serialize decimal to string",
            timer);
        break;

      case LONG:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.long2string"),
            "Serialize long to string",
            timer);
        break;

      case DOUBLE:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.double2string"),
            "Serialize double to string",
            timer);
        break;

      case SHORT:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.short2string"),
            "Serialize short to string",
            timer);
        break;

      case BYTE:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.byte2string"),
            "Serialize byte to string",
            timer);
        break;

      case BINARY:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.binary2string"),
            "Serialize binary to string",
            timer);
        break;

      case DATE:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.date2string"),
            "Serialize date to string",
            timer);
        break;

      case DATETIME:
        simpleValueToStream(iBuffer, iType, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.datetime2string"),
            "Serialize datetime to string",
            timer);
        break;

      case LINK:
        if (iValue instanceof YTRecordId) {
          ((YTRecordId) iValue).toString(iBuffer);
        } else {
          ((YTIdentifiable) iValue).getIdentity().toString(iBuffer);
        }
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.link2string"),
            "Serialize link to string",
            timer);
        break;

      case EMBEDDEDSET:
        ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionToStream(
            ODatabaseRecordThreadLocal.instance().getIfDefined(),
            iBuffer,
            null,
            null,
            iValue,
            true,
            true);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedSet2string"),
            "Serialize embeddedset to string",
            timer);
        break;

      case EMBEDDEDLIST:
        ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedCollectionToStream(
            ODatabaseRecordThreadLocal.instance().getIfDefined(),
            iBuffer,
            null,
            null,
            iValue,
            true,
            false);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedList2string"),
            "Serialize embeddedlist to string",
            timer);
        break;

      case EMBEDDEDMAP:
        ORecordSerializerSchemaAware2CSV.INSTANCE.embeddedMapToStream(
            ODatabaseRecordThreadLocal.instance().getIfDefined(),
            iBuffer,
            null,
            null,
            iValue,
            true);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embedMap2string"),
            "Serialize embeddedmap to string",
            timer);
        break;

      case EMBEDDED:
        if (iValue instanceof YTEntityImpl) {
          ORecordSerializerSchemaAware2CSV.INSTANCE.toString((YTEntityImpl) iValue, iBuffer, null);
        } else {
          OStringSerializerEmbedded.INSTANCE.toStream(iBuffer, iValue);
        }
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.embed2string"),
            "Serialize embedded to string",
            timer);
        break;

      case CUSTOM:
        OStringSerializerAnyStreamable.INSTANCE.toStream(iBuffer, iValue);
        PROFILER.stopChrono(
            PROFILER.getProcessMetric("serializer.record.string.custom2string"),
            "Serialize custom to string",
            timer);
        break;

      default:
        throw new IllegalArgumentException(
            "Type " + iType + " not supported to convert value: " + iValue);
    }
  }

  /**
   * Parses a string returning the closer type. Numbers by default are INTEGER if haven't decimal
   * separator, otherwise FLOAT. To treat all the number types numbers are postponed with a
   * character that tells the type: b=byte, s=short, l=long, f=float, d=double, t=date.
   *
   * @param iValue Value to parse
   * @return The closest type recognized
   */
  public static YTType getType(final String iValue) {
    if (iValue.length() == 0) {
      return null;
    }

    final char firstChar = iValue.charAt(0);

    if (firstChar == YTRID.PREFIX)
    // RID
    {
      return YTType.LINK;
    } else if (firstChar == '\'' || firstChar == '"') {
      return YTType.STRING;
    } else if (firstChar == OStringSerializerHelper.BINARY_BEGINEND) {
      return YTType.BINARY;
    } else if (firstChar == OStringSerializerHelper.EMBEDDED_BEGIN) {
      return YTType.EMBEDDED;
    } else if (firstChar == OStringSerializerHelper.LIST_BEGIN) {
      return YTType.EMBEDDEDLIST;
    } else if (firstChar == OStringSerializerHelper.SET_BEGIN) {
      return YTType.EMBEDDEDSET;
    } else if (firstChar == OStringSerializerHelper.MAP_BEGIN) {
      return YTType.EMBEDDEDMAP;
    } else if (firstChar == OStringSerializerHelper.CUSTOM_TYPE) {
      return YTType.CUSTOM;
    }

    // BOOLEAN?
    if (iValue.equalsIgnoreCase("true") || iValue.equalsIgnoreCase("false")) {
      return YTType.BOOLEAN;
    }

    // NUMBER OR STRING?
    boolean integer = true;
    for (int index = 0; index < iValue.length(); ++index) {
      final char c = iValue.charAt(index);
      if (c < '0' || c > '9') {
        if ((index == 0 && (c == '+' || c == '-'))) {
          continue;
        } else if (c == DECIMAL_SEPARATOR) {
          integer = false;
        } else {
          if (index > 0) {
            if (!integer && c == 'E') {
              // CHECK FOR SCIENTIFIC NOTATION
              if (index < iValue.length()) {
                if (iValue.charAt(index + 1) == '-')
                // JUMP THE DASH IF ANY (NOT MANDATORY)
                {
                  index++;
                }
                continue;
              }
            } else if (c == 'f') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.FLOAT;
            } else if (c == 'c') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.DECIMAL;
            } else if (c == 'l') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.LONG;
            } else if (c == 'd') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.DOUBLE;
            } else if (c == 'b') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.BYTE;
            } else if (c == 'a') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.DATE;
            } else if (c == 't') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.DATETIME;
            } else if (c == 's') {
              return index != (iValue.length() - 1) ? YTType.STRING : YTType.SHORT;
            } else if (c == 'e') { // eg. 1e-06
              try {
                Double.parseDouble(iValue);
                return YTType.DOUBLE;
              } catch (Exception ignore) {
                return YTType.STRING;
              }
            }
          }

          return YTType.STRING;
        }
      }
    }

    if (integer) {
      // AUTO CONVERT TO LONG IF THE INTEGER IS TOO BIG
      final int numberLength = iValue.length();
      if (numberLength > MAX_INTEGER_DIGITS
          || (numberLength == MAX_INTEGER_DIGITS && iValue.compareTo(MAX_INTEGER_AS_STRING) > 0)) {
        return YTType.LONG;
      }

      return YTType.INTEGER;
    }

    // CHECK IF THE DECIMAL NUMBER IS A FLOAT OR DOUBLE
    final double dou = Double.parseDouble(iValue);
    if (dou <= Float.MAX_VALUE
        && dou >= Float.MIN_VALUE
        && Double.toString(dou).equals(Float.toString((float) dou))
        && (double) Double.valueOf(dou).floatValue() == dou) {
      return YTType.FLOAT;
    } else if (!Double.toString(dou).equals(iValue)) {
      return YTType.DECIMAL;
    }

    return YTType.DOUBLE;
  }

  /**
   * Parses a string returning the value with the closer type. Numbers by default are INTEGER if
   * haven't decimal separator, otherwise FLOAT. To treat all the number types numbers are postponed
   * with a character that tells the type: b=byte, s=short, l=long, f=float, d=double, t=date. If
   * starts with # it's a RecordID. Most of the code is equals to getType() but has been copied to
   * speed-up it.
   *
   * @param db
   * @param iValue Value to parse
   * @return The closest type recognized
   */
  public static Object getTypeValue(YTDatabaseSessionInternal db, final String iValue) {
    if (iValue == null || iValue.equalsIgnoreCase("NULL")) {
      return null;
    }

    if (iValue.length() == 0) {
      return "";
    }

    if (iValue.length() > 1) {
      if (iValue.charAt(0) == '"' && iValue.charAt(iValue.length() - 1) == '"')
      // STRING
      {
        return OStringSerializerHelper.decode(iValue.substring(1, iValue.length() - 1));
      } else if (iValue.charAt(0) == OStringSerializerHelper.BINARY_BEGINEND
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.BINARY_BEGINEND)
      // STRING
      {
        return OStringSerializerHelper.getBinaryContent(iValue);
      } else if (iValue.charAt(0) == OStringSerializerHelper.LIST_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.LIST_END) {
        // LIST
        final ArrayList<String> coll = new ArrayList<String>();
        OStringSerializerHelper.getCollection(
            iValue,
            0,
            coll,
            OStringSerializerHelper.LIST_BEGIN,
            OStringSerializerHelper.LIST_END,
            OStringSerializerHelper.COLLECTION_SEPARATOR);
        return coll;
      } else if (iValue.charAt(0) == OStringSerializerHelper.SET_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.SET_END) {
        // SET
        final Set<String> coll = new HashSet<String>();
        OStringSerializerHelper.getCollection(
            iValue,
            0,
            coll,
            OStringSerializerHelper.SET_BEGIN,
            OStringSerializerHelper.SET_END,
            OStringSerializerHelper.COLLECTION_SEPARATOR);
        return coll;
      } else if (iValue.charAt(0) == OStringSerializerHelper.MAP_BEGIN
          && iValue.charAt(iValue.length() - 1) == OStringSerializerHelper.MAP_END) {
        // MAP
        return OStringSerializerHelper.getMap(db, iValue);
      }
    }

    if (iValue.charAt(0) == YTRID.PREFIX)
    // RID
    {
      return new YTRecordId(iValue);
    }

    boolean integer = true;
    char c;

    boolean stringStarBySign = false;

    for (int index = 0; index < iValue.length(); ++index) {
      c = iValue.charAt(index);
      if (c < '0' || c > '9') {
        if ((index == 0 && (c == '+' || c == '-'))) {
          stringStarBySign = true;
          continue;
        } else if (c == DECIMAL_SEPARATOR) {
          integer = false;
        } else {
          if (index > 0) {
            if (!integer && c == 'E') {
              // CHECK FOR SCIENTIFIC NOTATION
              if (index < iValue.length()) {
                index++;
              }
              if (iValue.charAt(index) == '-') {
                continue;
              }
            }

            final String v = iValue.substring(0, index);

            if (c == 'f') {
              return Float.valueOf(v);
            } else if (c == 'c') {
              return new BigDecimal(v);
            } else if (c == 'l') {
              return Long.valueOf(v);
            } else if (c == 'd') {
              return Double.valueOf(v);
            } else if (c == 'b') {
              return Byte.valueOf(v);
            } else if (c == 'a' || c == 't') {
              return new Date(Long.parseLong(v));
            } else if (c == 's') {
              return Short.valueOf(v);
            }
          }
          return iValue;
        }
      } else if (stringStarBySign) {
        stringStarBySign = false;
      }
    }
    if (stringStarBySign) {
      return iValue;
    }

    if (integer) {
      try {
        return Integer.valueOf(iValue);
      } catch (NumberFormatException ignore) {
        return Long.valueOf(iValue);
      }
    } else if ("NaN".equals(iValue) || "Infinity".equals(iValue))
    // NaN and Infinity CANNOT BE MANAGED BY BIG-DECIMAL TYPE
    {
      return Double.valueOf(iValue);
    } else {
      return new BigDecimal(iValue);
    }
  }

  public static Object simpleValueFromStream(YTDatabaseSessionInternal db, final Object iValue,
      final YTType iType) {
    switch (iType) {
      case STRING:
        if (iValue instanceof String) {
          final String s = OIOUtils.getStringContent(iValue);
          return OStringSerializerHelper.decode(s);
        }
        return iValue.toString();

      case INTEGER:
        if (iValue instanceof Integer) {
          return iValue;
        }
        return Integer.valueOf(iValue.toString());

      case BOOLEAN:
        if (iValue instanceof Boolean) {
          return iValue;
        }
        return Boolean.valueOf(iValue.toString());

      case FLOAT:
        if (iValue instanceof Float) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case DECIMAL:
        if (iValue instanceof BigDecimal) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case LONG:
        if (iValue instanceof Long) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case DOUBLE:
        if (iValue instanceof Double) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case SHORT:
        if (iValue instanceof Short) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case BYTE:
        if (iValue instanceof Byte) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case BINARY:
        return OStringSerializerHelper.getBinaryContent(iValue);

      case DATE:
      case DATETIME:
        if (iValue instanceof Date) {
          return iValue;
        }
        return convertValue(db, (String) iValue, iType);

      case LINK:
        if (iValue instanceof YTRID) {
          return iValue.toString();
        } else if (iValue instanceof String) {
          return new YTRecordId((String) iValue);
        } else {
          return ((YTRecord) iValue).getIdentity().toString();
        }
    }

    throw new IllegalArgumentException("Type " + iType + " is not simple type.");
  }

  public static void simpleValueToStream(
      final StringBuilder iBuffer, final YTType iType, final Object iValue) {
    if (iValue == null || iType == null) {
      return;
    }
    switch (iType) {
      case STRING:
        iBuffer.append('"');
        iBuffer.append(OStringSerializerHelper.encode(iValue.toString()));
        iBuffer.append('"');
        break;

      case BOOLEAN:
        iBuffer.append(iValue);
        break;

      case INTEGER:
        iBuffer.append(iValue);
        break;

      case FLOAT:
        iBuffer.append(iValue);
        iBuffer.append('f');
        break;

      case DECIMAL:
        if (iValue instanceof BigDecimal) {
          iBuffer.append(((BigDecimal) iValue).toPlainString());
        } else {
          iBuffer.append(iValue);
        }
        iBuffer.append('c');
        break;

      case LONG:
        iBuffer.append(iValue);
        iBuffer.append('l');
        break;

      case DOUBLE:
        iBuffer.append(iValue);
        iBuffer.append('d');
        break;

      case SHORT:
        iBuffer.append(iValue);
        iBuffer.append('s');
        break;

      case BYTE:
        if (iValue instanceof Character) {
          iBuffer.append((int) ((Character) iValue).charValue());
        } else if (iValue instanceof String) {
          iBuffer.append((int) ((String) iValue).charAt(0));
        } else {
          iBuffer.append(iValue);
        }
        iBuffer.append('b');
        break;

      case BINARY:
        iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
        if (iValue instanceof Byte) {
          iBuffer.append(
              Base64.getEncoder().encodeToString(new byte[]{((Byte) iValue).byteValue()}));
        } else {
          iBuffer.append(Base64.getEncoder().encodeToString((byte[]) iValue));
        }
        iBuffer.append(OStringSerializerHelper.BINARY_BEGINEND);
        break;

      case DATE:
        if (iValue instanceof Date) {
          // RESET HOURS, MINUTES, SECONDS AND MILLISECONDS
          final Calendar calendar = ODateHelper.getDatabaseCalendar();
          calendar.setTime((Date) iValue);
          calendar.set(Calendar.HOUR_OF_DAY, 0);
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
          calendar.set(Calendar.MILLISECOND, 0);

          iBuffer.append(calendar.getTimeInMillis());
        } else {
          iBuffer.append(iValue);
        }
        iBuffer.append('a');
        break;

      case DATETIME:
        if (iValue instanceof Date) {
          iBuffer.append(((Date) iValue).getTime());
        } else {
          iBuffer.append(iValue);
        }
        iBuffer.append('t');
        break;
    }
  }

  public abstract YTRecordAbstract fromString(
      YTDatabaseSessionInternal db, String iContent, YTRecordAbstract iRecord, String[] iFields);

  public StringBuilder toString(
      final YTRecord iRecord, final StringBuilder iOutput, final String iFormat) {
    return toString(iRecord, iOutput, iFormat, true);
  }

  public YTRecord fromString(YTDatabaseSessionInternal db, final String iSource) {
    return fromString(db, iSource, ODatabaseRecordThreadLocal.instance().get().newInstance(), null);
  }

  @Override
  public String[] getFieldNames(YTDatabaseSessionInternal db, YTEntityImpl reference,
      byte[] iSource) {
    return null;
  }

  @Override
  public YTRecordAbstract fromStream(
      YTDatabaseSessionInternal db, final byte[] iSource, final YTRecordAbstract iRecord,
      final String[] iFields) {
    final long timer = PROFILER.startChrono();

    try {
      return fromString(db, new String(iSource, StandardCharsets.UTF_8), iRecord, iFields);
    } finally {

      PROFILER.stopChrono(
          PROFILER.getProcessMetric("serializer.record.string.fromStream"),
          "Deserialize record from stream",
          timer);
    }
  }

  public byte[] toStream(YTDatabaseSessionInternal session, final YTRecordAbstract iRecord) {
    final long timer = PROFILER.startChrono();

    try {
      return toString(iRecord, new StringBuilder(2048), null, true)
          .toString()
          .getBytes(StandardCharsets.UTF_8);
    } finally {

      PROFILER.stopChrono(
          PROFILER.getProcessMetric("serializer.record.string.toStream"),
          "Serialize record to stream",
          timer);
    }
  }

  protected abstract StringBuilder toString(
      final YTRecord iRecord,
      final StringBuilder iOutput,
      final String iFormat,
      boolean autoDetectCollectionType);

  public boolean getSupportBinaryEvaluate() {
    return false;
  }
}
