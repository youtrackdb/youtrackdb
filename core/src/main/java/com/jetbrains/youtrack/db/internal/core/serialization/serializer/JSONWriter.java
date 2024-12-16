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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.collection.MultiCollectionIterator;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.util.DateHelper;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.io.StringWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.math.BigDecimal;
import java.util.Base64;
import java.util.Collection;
import java.util.Date;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

@SuppressWarnings("unchecked")
public class JSONWriter {

  private static final String DEF_FORMAT =
      "rid,type,version,class,attribSameRow,indent:2,dateAsLong"; // TODO: added
  private final String format;
  private final Writer out;
  private boolean prettyPrint = false;
  private boolean firstAttribute = true;

  public JSONWriter(final Writer out) {
    this(out, DEF_FORMAT);
  }

  public JSONWriter(final Writer out, final String iJsonFormat) {
    this.out = out;
    this.format = iJsonFormat;
    if (iJsonFormat.contains("prettyPrint")) {
      prettyPrint = true;
    }
  }

  public static String writeValue(final Object iValue) throws IOException {
    return writeValue(iValue, DEF_FORMAT);
  }

  public static String writeValue(Object iValue, final String iFormat) throws IOException {
    return writeValue(iValue, iFormat, 0, null);
  }

  public static String writeValue(
      Object iValue, final String iFormat, final int iIndentLevel, PropertyType valueType)
      throws IOException {
    if (iValue == null) {
      return "null";
    }

    final StringBuilder buffer = new StringBuilder(64);
    if (iValue instanceof Boolean || iValue instanceof Number) {

      if (iValue instanceof Double && !Double.isFinite((Double) iValue)) {
        buffer.append("null");
      } else if ((iValue instanceof Float && !Float.isFinite((Float) iValue))) {
        buffer.append("null");
      } else {
        buffer.append(iValue);
      }

    } else if (iValue instanceof Identifiable linked) {
      if (((RecordId) linked.getIdentity()).isValid()) {
        buffer.append('\"');
        ((RecordId) linked.getIdentity()).toString(buffer);
        buffer.append('\"');
      } else {
        if (iFormat != null && iFormat.contains("shallow")) {
          buffer.append("{}");
        } else {
          try {
            final Record rec = linked.getRecord();
            final String embeddedFormat =
                iFormat != null && iFormat.isEmpty()
                    ? "indent:" + iIndentLevel
                    : iFormat + ",indent:" + iIndentLevel;
            buffer.append(rec.toJSON(embeddedFormat));
          } catch (RecordNotFoundException e) {
            buffer.append("null");
          }
        }
      }

    } else if (iValue.getClass().isArray()) {

      if (iValue instanceof byte[] source) {
        buffer.append('\"');

        if (iFormat != null && iFormat.contains("shallow")) {
          buffer.append(source.length);
        } else {
          buffer.append(Base64.getEncoder().encodeToString(source));
        }

        buffer.append('\"');
      } else {
        buffer.append('[');
        int size = Array.getLength(iValue);
        if (iFormat != null && iFormat.contains("shallow")) {
          buffer.append(size);
        } else {
          for (int i = 0; i < size; ++i) {
            if (i > 0) {
              buffer.append(",");
            }
            buffer.append(writeValue(Array.get(iValue, i), iFormat));
          }
        }
        buffer.append(']');
      }
    } else if (iValue instanceof Iterator<?>) {
      iteratorToJSON((Iterator<?>) iValue, iFormat, buffer);
    } else if (iValue instanceof Iterable<?>) {
      iteratorToJSON(((Iterable<?>) iValue).iterator(), iFormat, buffer);
    } else if (iValue instanceof Map<?, ?>) {
      mapToJSON((Map<Object, Object>) iValue, iFormat, buffer);
    } else if (iValue instanceof Map.Entry<?, ?> entry) {
      buffer.append('{');
      buffer.append(writeValue(entry.getKey(), iFormat));
      buffer.append(":");
      if (iFormat.contains("prettyPrint")) {
        buffer.append(' ');
      }
      buffer.append(writeValue(entry.getValue(), iFormat));
      buffer.append('}');
    } else if (iValue instanceof Date) {
      if (iFormat.indexOf("dateAsLong") > -1) {
        buffer.append(((Date) iValue).getTime());
      } else {
        buffer.append('"');
        buffer.append(DateHelper.getDateTimeFormatInstance().format(iValue));
        buffer.append('"');
      }
    } else if (iValue instanceof BigDecimal) {
      buffer.append(((BigDecimal) iValue).toPlainString());
    } else if (iValue instanceof Iterable<?>) {
      iteratorToJSON(((Iterable<?>) iValue).iterator(), iFormat, buffer);
    } else {
      if (valueType == null) {
        valueType = PropertyType.getTypeByValue(iValue);
      }

      if (valueType == PropertyType.CUSTOM) {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream object = new ObjectOutputStream(baos);
        object.writeObject(iValue);
        object.flush();
        buffer.append('"');
        buffer.append(Base64.getEncoder().encodeToString(baos.toByteArray()));
        buffer.append('"');
      } else {
        // TREAT IT AS STRING
        final String v = iValue.toString();
        buffer.append('"');
        buffer.append(encode(v));
        buffer.append('"');
      }
    }

    return buffer.toString();
  }

  protected static void iteratorToJSON(
      final Iterator<?> it, final String iFormat, final StringBuilder buffer) throws IOException {
    buffer.append('[');
    if (iFormat != null && iFormat.contains("shallow")) {
      if (it instanceof MultiCollectionIterator<?>) {
        buffer.append(((MultiCollectionIterator<?>) it).size());
      } else {
        // COUNT THE MULTI VALUE
        int i;
        for (i = 0; it.hasNext(); ++i) {
          it.next();
        }
        buffer.append(i);
      }
    } else {
      for (int i = 0; it.hasNext(); ++i) {
        if (i > 0) {
          buffer.append(",");
        }
        buffer.append(writeValue(it.next(), iFormat));
      }
    }
    buffer.append(']');
  }

  public static Object encode(final Object iValue) {
    if (iValue instanceof String) {
      return IOUtils.encodeJsonString((String) iValue);
    } else {
      return iValue;
    }
  }

  public static String listToJSON(
      final Collection<? extends Identifiable> iRecords, final String iFormat) {
    try {
      final StringWriter buffer = new StringWriter();
      final JSONWriter json = new JSONWriter(buffer);
      // WRITE RECORDS
      json.beginCollection(0, false, null);
      if (iRecords != null) {
        if (iFormat != null && iFormat.contains("shallow")) {
          buffer.append("" + iRecords.size());
        } else {
          int counter = 0;
          String objectJson;
          for (Identifiable rec : iRecords) {
            if (rec != null) {
              try {
                objectJson =
                    iFormat != null ? rec.getRecord().toJSON(iFormat) : rec.getRecord().toJSON();

                if (counter++ > 0) {
                  buffer.append(",");
                }

                buffer.append(objectJson);
              } catch (Exception e) {
                LogManager.instance()
                    .error(json, "Error transforming record " + rec.getIdentity() + " to JSON", e);
              }
            }
          }
        }
      }
      json.endCollection(0, false);

      return buffer.toString();
    } catch (IOException e) {
      throw BaseException.wrapException(
          new SerializationException("Error on serializing collection"), e);
    }
  }

  public static String mapToJSON(Map<?, ?> iMap) {
    return mapToJSON(iMap, null, new StringBuilder(128));
  }

  public static String mapToJSON(
      final Map<?, ?> iMap, final String iFormat, final StringBuilder buffer) {
    try {
      buffer.append('{');
      if (iMap != null) {
        int i = 0;
        Entry<?, ?> entry;
        for (Iterator<?> it = iMap.entrySet().iterator(); it.hasNext(); ++i) {
          entry = (Entry<?, ?>) it.next();
          if (i > 0) {
            buffer.append(",");
          }
          buffer.append(writeValue(entry.getKey(), iFormat));
          buffer.append(":");
          buffer.append(writeValue(entry.getValue(), iFormat));
        }
      }
      buffer.append('}');
      return buffer.toString();
    } catch (IOException e) {
      throw BaseException.wrapException(new SerializationException("Error on serializing map"), e);
    }
  }

  public JSONWriter beginObject() throws IOException {
    beginObject(0, false, null);
    return this;
  }

  public JSONWriter beginObject(final int iIdentLevel) throws IOException {
    beginObject(iIdentLevel, false, null);
    return this;
  }

  public JSONWriter beginObject(final Object iName) throws IOException {
    beginObject(-1, false, iName);
    return this;
  }

  public JSONWriter beginObject(final int iIdentLevel, final boolean iNewLine, final Object iName)
      throws IOException {
    if (!firstAttribute) {
      out.append(",");
    }

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append("\"" + iName + "\":");
      if (prettyPrint) {
        out.append(' ');
      }
    }

    out.append('{');

    firstAttribute = true;
    return this;
  }

  public JSONWriter writeRecord(
      final int iIdentLevel, final boolean iNewLine, final Object iName, final Record iRecord)
      throws IOException {
    if (!firstAttribute) {
      out.append(",");
    }

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append("\"" + iName + "\":");
      if (prettyPrint) {
        out.append(' ');
      }
    }

    out.append(iRecord.toJSON(format));

    firstAttribute = false;
    return this;
  }

  public JSONWriter endObject() throws IOException {
    format(-1, true);
    out.append('}');
    return this;
  }

  public JSONWriter endObject(final int iIdentLevel) throws IOException {
    return endObject(iIdentLevel, true);
  }

  public JSONWriter endObject(final int iIdentLevel, final boolean iNewLine) throws IOException {
    format(iIdentLevel, iNewLine);
    out.append('}');
    firstAttribute = false;
    return this;
  }

  public JSONWriter beginCollection(final String iName) throws IOException {
    return beginCollection(-1, false, iName);
  }

  public JSONWriter beginCollection(
      final int iIdentLevel, final boolean iNewLine, final String iName) throws IOException {
    if (!firstAttribute) {
      out.append(",");
    }

    format(iIdentLevel, iNewLine);

    if (iName != null && !iName.isEmpty()) {
      out.append(writeValue(iName, format));
      out.append(":");
      if (prettyPrint) {
        out.append(' ');
      }
    }
    out.append("[");

    firstAttribute = true;
    return this;
  }

  public JSONWriter endCollection() throws IOException {
    return endCollection(-1, false);
  }

  public JSONWriter endCollection(final int iIdentLevel, final boolean iNewLine)
      throws IOException {
    format(iIdentLevel, iNewLine);
    firstAttribute = false;
    out.append(']');
    return this;
  }

  public JSONWriter writeObjects(final String iName, Object[]... iPairs) throws IOException {
    return writeObjects(-1, false, iName, iPairs);
  }

  public JSONWriter writeObjects(
      int iIdentLevel, boolean iNewLine, final String iName, Object[]... iPairs)
      throws IOException {
    for (int i = 0; i < iPairs.length; ++i) {
      beginObject(iIdentLevel, true, iName);
      for (int k = 0; k < iPairs[i].length; ) {
        writeAttribute(iIdentLevel + 1, false, (String) iPairs[i][k++], iPairs[i][k++], format);
      }
      endObject(iIdentLevel, false);
    }
    return this;
  }

  public JSONWriter writeAttribute(final String iName, final Object iValue) throws IOException {
    return writeAttribute(-1, false, iName, iValue, format);
  }

  public JSONWriter writeAttribute(
      final int iIdentLevel, final boolean iNewLine, final String iName, final Object iValue)
      throws IOException {
    return writeAttribute(iIdentLevel, iNewLine, iName, iValue, format, null);
  }

  public JSONWriter writeAttribute(
      final int iIdentLevel,
      final boolean iNewLine,
      final String iName,
      final Object iValue,
      final String iFormat)
      throws IOException {
    return writeAttribute(iIdentLevel, iNewLine, iName, iValue, iFormat, null);
  }

  public JSONWriter writeAttribute(
      final int iIdentLevel,
      final boolean iNewLine,
      final String iName,
      final Object iValue,
      final String iFormat,
      PropertyType valueType)
      throws IOException {
    if (!firstAttribute) {
      out.append(",");
    }

    format(iIdentLevel, iNewLine);

    if (iName != null) {
      out.append(writeValue(iName, iFormat));
      out.append(":");
      if (prettyPrint) {
        out.append(' ');
      }
    }

    if (iFormat != null
        && iFormat.contains("graph")
        && iName != null
        && (iName.startsWith("in_") || iName.startsWith("out_"))
        && (iValue == null || iValue instanceof Identifiable)) {
      // FORCE THE OUTPUT AS COLLECTION
      out.append('[');
      if (iValue instanceof Identifiable) {
        final boolean shallow = iFormat != null && iFormat.contains("shallow");
        if (shallow) {
          out.append("1");
        } else {
          out.append(writeValue(iValue, iFormat));
        }
      }
      out.append(']');
    } else {
      out.append(writeValue(iValue, iFormat, iIdentLevel, valueType));
    }

    firstAttribute = false;
    return this;
  }

  public JSONWriter writeValue(final int iIdentLevel, final boolean iNewLine, final Object iValue)
      throws IOException {
    if (!firstAttribute) {
      out.append(",");
    }

    format(iIdentLevel, iNewLine);

    out.append(writeValue(iValue, format));

    firstAttribute = false;
    return this;
  }

  public JSONWriter flush() throws IOException {
    out.flush();
    return this;
  }

  public JSONWriter close() throws IOException {
    out.close();
    return this;
  }

  public JSONWriter append(final String iText) throws IOException {
    out.append(iText);
    return this;
  }

  public boolean isPrettyPrint() {
    return prettyPrint;
  }

  public JSONWriter setPrettyPrint(boolean prettyPrint) {
    this.prettyPrint = prettyPrint;
    return this;
  }

  public void write(final String iText) throws IOException {
    out.append(iText);
  }

  public void newline() throws IOException {
    if (prettyPrint) {
      out.append("\r\n");
    }
  }

  public void resetAttributes() {
    firstAttribute = true;
  }

  private JSONWriter format(final int iIdentLevel, final boolean iNewLine) throws IOException {
    if (iIdentLevel > -1) {
      if (iNewLine) {
        newline();
      }

      if (prettyPrint) {
        for (int i = 0; i < iIdentLevel; ++i) {
          out.append("  ");
        }
      }
    }
    return this;
  }
}
