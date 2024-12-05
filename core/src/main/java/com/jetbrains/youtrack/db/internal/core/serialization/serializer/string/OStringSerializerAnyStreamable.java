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

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.OSerializableStream;
import java.util.Base64;

public class OStringSerializerAnyStreamable implements OStringSerializer {

  public static final OStringSerializerAnyStreamable INSTANCE =
      new OStringSerializerAnyStreamable();
  public static final String NAME = "st";

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique
   * parameter.
   */
  public Object fromStream(YTDatabaseSessionInternal db, final String iStream) {
    if (iStream == null || iStream.length() == 0)
    // NULL VALUE
    {
      return null;
    }

    OSerializableStream instance = null;

    int propertyPos = iStream.indexOf(':');
    int pos = iStream.indexOf(OStringSerializerEmbedded.SEPARATOR);
    if (pos < 0 || propertyPos > -1 && pos > propertyPos) {
      instance = new EntityImpl();
      pos = -1;
    } else {
      final String className = iStream.substring(0, pos);
      try {
        final Class<?> clazz = Class.forName(className);
        instance = (OSerializableStream) clazz.newInstance();
      } catch (Exception e) {
        final String message = "Error on unmarshalling content. Class: " + className;
        LogManager.instance().error(this, message, e);
        throw YTException.wrapException(new YTSerializationException(message), e);
      }
    }

    instance.fromStream(Base64.getDecoder().decode(iStream.substring(pos + 1)));
    return instance;
  }

  /**
   * Serialize the class name size + class name + object content
   *
   * @param iValue
   */
  public StringBuilder toStream(final StringBuilder iOutput, Object iValue) {
    if (iValue != null) {
      if (!(iValue instanceof OSerializableStream stream)) {
        throw new YTSerializationException(
            "Cannot serialize the object since it's not implements the OSerializableStream"
                + " interface");
      }

      iOutput.append(iValue.getClass().getName());
      iOutput.append(OStringSerializerEmbedded.SEPARATOR);
      iOutput.append(Base64.getEncoder().encodeToString(stream.toStream()));
    }
    return iOutput;
  }

  public String getName() {
    return NAME;
  }
}
