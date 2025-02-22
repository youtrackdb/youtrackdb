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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.string;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImplEmbedded;
import com.jetbrains.youtrack.db.internal.core.serialization.DocumentSerializable;
import com.jetbrains.youtrack.db.internal.core.serialization.SerializableStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerSchemaAware2CSV;
import java.nio.charset.StandardCharsets;

public class StringSerializerEmbedded implements StringSerializer {

  public static final StringSerializerEmbedded INSTANCE = new StringSerializerEmbedded();
  public static final String NAME = "em";
  public static final String SEPARATOR = "|";
  public static final char SHORT_FORM_PREFIX = '!';

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique
   * parameter.
   */
  public Object fromStream(DatabaseSessionInternal db, final String iStream) {
    if (iStream == null || iStream.isEmpty())
    // NULL VALUE
    {
      return null;
    }

    final EntityImpl instance = new EntityImplEmbedded();
    RecordSerializerSchemaAware2CSV.INSTANCE.fromStream(db,
        iStream.getBytes(StandardCharsets.UTF_8), instance, null);

    final String className = instance.field(DocumentSerializable.CLASS_NAME);
    if (className == null) {
      return instance;
    }

    Class<?> clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      LogManager.instance()
          .debug(
              this,
              "Class name provided in embedded entity " + className + " does not exist.",
              e);
    }

    if (clazz == null) {
      return instance;
    }

    if (DocumentSerializable.class.isAssignableFrom(clazz)) {
      try {
        final DocumentSerializable documentSerializable =
            (DocumentSerializable) clazz.newInstance();
        final EntityImpl docClone = new EntityImplEmbedded();
        instance.copyTo(docClone);
        docClone.removeField(DocumentSerializable.CLASS_NAME);
        documentSerializable.fromDocument(docClone);

        return documentSerializable;
      } catch (InstantiationException e) {
        throw BaseException.wrapException(
            new SerializationException("Cannot serialize the object"), e);
      } catch (IllegalAccessException e) {
        throw BaseException.wrapException(
            new SerializationException("Cannot serialize the object"), e);
      }
    }

    return instance;
  }

  /**
   * Serialize the class name size + class name + object content
   */
  public StringBuilder toStream(final StringBuilder iOutput, Object iValue) {
    if (iValue != null) {
      if (iValue instanceof DocumentSerializable) {
        iValue = ((DocumentSerializable) iValue).toDocument();
      }

      if (!(iValue instanceof SerializableStream stream)) {
        throw new SerializationException(
            "Cannot serialize the object since it's not implements the SerializableStream"
                + " interface");
      }

      iOutput.append(iValue.getClass().getName());
      iOutput.append(SEPARATOR);
      iOutput.append(new String(stream.toStream(), StandardCharsets.UTF_8));
    }

    return iOutput;
  }

  public String getName() {
    return NAME;
  }
}
