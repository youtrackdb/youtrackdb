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
package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.ExecutionThreadLocal;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.index.CompositeKey;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.MemoryStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.StringSerializerHelper;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.index.CompositeKeySerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.RecordSerializer;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.string.RecordSerializerStringAbstract;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import javax.annotation.Nonnull;

/**
 * Text based Command Request abstract class.
 */
@SuppressWarnings("serial")
public abstract class CommandRequestTextAbstract extends CommandRequestAbstract
    implements CommandRequestText {

  protected String text;

  protected CommandRequestTextAbstract() {
  }

  protected CommandRequestTextAbstract(final String iText) {
    if (iText == null) {
      throw new IllegalArgumentException("Text cannot be null");
    }

    text = iText.trim();
  }

  /**
   * Delegates the execution to the configured command executor.
   */
  @SuppressWarnings("unchecked")
  public <RET> RET execute(@Nonnull DatabaseSessionInternal querySession, final Object... iArgs) {
    setParameters(iArgs);

    ExecutionThreadLocal.INSTANCE.get().onAsyncReplicationOk = onAsyncReplicationOk;
    ExecutionThreadLocal.INSTANCE.get().onAsyncReplicationError = onAsyncReplicationError;

    if (context == null) {
      context = new BasicCommandContext();
    }

    context.setDatabase(querySession);
    return (RET) querySession.getStorage()
        .command(querySession, this);
  }

  public String getText() {
    return text;
  }

  public CommandRequestText setText(final String iText) {
    this.text = iText;
    return this;
  }

  public CommandRequestText fromStream(DatabaseSessionInternal db, final byte[] iStream,
      RecordSerializerNetwork serializer)
      throws SerializationException {
    final MemoryStream buffer = new MemoryStream(iStream);
    fromStream(db, buffer, serializer);
    return this;
  }

  public byte[] toStream(DatabaseSessionInternal db, RecordSerializerNetwork serializer)
      throws SerializationException {
    final MemoryStream buffer = new MemoryStream();
    return toStream(buffer);
  }

  @Override
  public String toString() {
    return "?." + text;
  }

  protected byte[] toStream(final MemoryStream buffer) {
    buffer.setUtf8(text);

    if (parameters == null || parameters.isEmpty()) {
      // simple params are absent
      buffer.set(false);
      // composite keys are absent
      buffer.set(false);
    } else {
      final Map<Object, Object> params = new HashMap<Object, Object>();
      final Map<Object, List<Object>> compositeKeyParams = new HashMap<Object, List<Object>>();

      for (final Entry<Object, Object> paramEntry : parameters.entrySet()) {
        if (paramEntry.getValue() instanceof CompositeKey compositeKey) {
          compositeKeyParams.put(paramEntry.getKey(), compositeKey.getKeys());
        } else {
          params.put(paramEntry.getKey(), paramEntry.getValue());
        }
      }

      buffer.set(!params.isEmpty());
      if (!params.isEmpty()) {
        final EntityImpl param = new EntityImpl(null);
        param.field("parameters", params);
        buffer.set(param.toStream());
      }

      buffer.set(!compositeKeyParams.isEmpty());
      if (!compositeKeyParams.isEmpty()) {
        final EntityImpl compositeKey = new EntityImpl(null);
        compositeKey.field("compositeKeyParams", compositeKeyParams);
        buffer.set(compositeKey.toStream());
      }
    }

    return buffer.toByteArray();
  }

  protected void fromStream(DatabaseSessionInternal db, final MemoryStream buffer,
      RecordSerializer serializer) {
    text = buffer.getAsString();

    parameters = null;

    final boolean simpleParams = buffer.getAsBoolean();
    if (simpleParams) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final EntityImpl param = new EntityImpl(null);
      if (serializer != null) {
        serializer.fromStream(db, paramBuffer, param, null);
      } else {
        param.fromStream(paramBuffer);
      }

      Map<Object, Object> params = param.field("params");
      parameters = new HashMap<Object, Object>();
      if (params != null) {
        for (Entry<Object, Object> p : params.entrySet()) {
          final Object value;
          if (p.getValue() instanceof String) {
            value = RecordSerializerStringAbstract.getTypeValue(db, (String) p.getValue());
          } else {
            value = p.getValue();
          }

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0))) {
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          } else {
            parameters.put(p.getKey(), value);
          }
        }
      } else {
        params = param.field("parameters");
        for (Entry<Object, Object> p : params.entrySet()) {
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0))) {
            parameters.put(Integer.parseInt((String) p.getKey()), p.getValue());
          } else {
            parameters.put(p.getKey(), p.getValue());
          }
        }
      }
    }

    final boolean compositeKeyParamsPresent = buffer.getAsBoolean();
    if (compositeKeyParamsPresent) {
      final byte[] paramBuffer = buffer.getAsByteArray();
      final EntityImpl param = new EntityImpl(null);
      if (serializer != null) {
        serializer.fromStream(db, paramBuffer, param, null);
      } else {
        param.fromStream(paramBuffer);
      }

      final Map<Object, Object> compositeKeyParams = param.field("compositeKeyParams");

      if (parameters == null) {
        parameters = new HashMap<Object, Object>();
      }

      for (final Entry<Object, Object> p : compositeKeyParams.entrySet()) {
        if (p.getValue() instanceof List) {
          final CompositeKey compositeKey = new CompositeKey((List<?>) p.getValue());
          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0))) {
            parameters.put(Integer.parseInt((String) p.getKey()), compositeKey);
          } else {
            parameters.put(p.getKey(), compositeKey);
          }

        } else {
          final Object value =
              CompositeKeySerializer.INSTANCE.deserialize(
                  StringSerializerHelper.getBinaryContent(p.getValue()), 0);

          if (p.getKey() instanceof String && Character.isDigit(((String) p.getKey()).charAt(0))) {
            parameters.put(Integer.parseInt((String) p.getKey()), value);
          } else {
            parameters.put(p.getKey(), value);
          }
        }
      }
    }
  }
}
