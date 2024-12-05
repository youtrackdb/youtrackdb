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
package com.jetbrains.youtrack.db.internal.core.serialization.serializer.stream;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.OArrays;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.script.CommandScript;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTSerializationException;
import com.jetbrains.youtrack.db.internal.core.serialization.OBinaryProtocol;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.ORecordSerializer;
import com.jetbrains.youtrack.db.internal.core.sql.CommandSQL;
import com.jetbrains.youtrack.db.internal.core.sql.query.LiveQuery;
import com.jetbrains.youtrack.db.internal.core.sql.query.OSQLSynchQuery;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class OStreamSerializerAnyStreamable {

  private static final String SCRIPT_COMMAND_CLASS = "s";
  private static final byte[] SCRIPT_COMMAND_CLASS_ASBYTES = SCRIPT_COMMAND_CLASS.getBytes();
  private static final String SQL_COMMAND_CLASS = "c";
  private static final byte[] SQL_COMMAND_CLASS_ASBYTES = SQL_COMMAND_CLASS.getBytes();
  private static final String QUERY_COMMAND_CLASS = "q";
  private static final byte[] QUERY_COMMAND_CLASS_ASBYTES = QUERY_COMMAND_CLASS.getBytes();

  public static final OStreamSerializerAnyStreamable INSTANCE =
      new OStreamSerializerAnyStreamable();
  public static final String NAME = "at";

  /**
   * Re-Create any object if the class has a public constructor that accepts a String as unique
   * parameter.
   */
  public CommandRequestText fromStream(YTDatabaseSessionInternal db, final byte[] iStream,
      ORecordSerializer serializer)
      throws IOException {
    if (iStream == null || iStream.length == 0)
    // NULL VALUE
    {
      return null;
    }

    final int classNameSize = OBinaryProtocol.bytes2int(iStream);

    if (classNameSize <= 0) {
      final String message =
          "Class signature not found in ANY element: " + Arrays.toString(iStream);
      LogManager.instance().error(this, message, null);

      throw new YTSerializationException(message);
    }

    final String className = new String(iStream, 4, classNameSize, StandardCharsets.UTF_8);

    try {
      final CommandRequestText stream;
      // CHECK FOR ALIASES
      if (className.equalsIgnoreCase("q"))
      // QUERY
      {
        stream = new OSQLSynchQuery<Object>();
      } else if (className.equalsIgnoreCase("c"))
      // SQL COMMAND
      {
        stream = new CommandSQL();
      } else if (className.equalsIgnoreCase("s"))
      // SCRIPT COMMAND
      {
        stream = new CommandScript();
      } else
      // CREATE THE OBJECT BY INVOKING THE EMPTY CONSTRUCTOR
      {
        stream = (CommandRequestText) Class.forName(className).newInstance();
      }

      return stream.fromStream(db,
          OArrays.copyOfRange(iStream, 4 + classNameSize, iStream.length), serializer);

    } catch (Exception e) {
      final String message = "Error on unmarshalling content. Class: " + className;
      LogManager.instance().error(this, message, e);
      throw YTException.wrapException(new YTSerializationException(message), e);
    }
  }

  /**
   * Serialize the class name size + class name + object content
   */
  public byte[] toStream(final CommandRequestText iObject) throws IOException {
    if (iObject == null) {
      return null;
    }

    // SERIALIZE THE CLASS NAME
    final byte[] className;
    if (iObject instanceof LiveQuery<?>) {
      className = iObject.getClass().getName().getBytes(StandardCharsets.UTF_8);
    } else if (iObject instanceof OSQLSynchQuery<?>) {
      className = QUERY_COMMAND_CLASS_ASBYTES;
    } else if (iObject instanceof CommandSQL) {
      className = SQL_COMMAND_CLASS_ASBYTES;
    } else if (iObject instanceof CommandScript) {
      className = SCRIPT_COMMAND_CLASS_ASBYTES;
    } else {
      if (iObject == null) {
        className = null;
      } else {
        className = iObject.getClass().getName().getBytes(StandardCharsets.UTF_8);
      }
    }
    // SERIALIZE THE OBJECT CONTENT
    byte[] objectContent = iObject.toStream();

    byte[] result = new byte[4 + className.length + objectContent.length];

    // COPY THE CLASS NAME SIZE + CLASS NAME + OBJECT CONTENT
    System.arraycopy(OBinaryProtocol.int2bytes(className.length), 0, result, 0, 4);
    System.arraycopy(className, 0, result, 4, className.length);
    System.arraycopy(objectContent, 0, result, 4 + className.length, objectContent.length);

    return result;
  }

  public String getName() {
    return NAME;
  }
}
