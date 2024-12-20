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
package com.jetbrains.youtrack.db.internal.core.sql.query;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.QueryParsingException;
import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.query.QueryAbstract;
import com.jetbrains.youtrack.db.internal.core.serialization.MemoryStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

/**
 * SQL query implementation.
 *
 * @param <T> Record type to return.
 */
public abstract class SQLQuery<T> extends QueryAbstract<T> implements CommandRequestText {

  protected String text;

  public SQLQuery() {
  }

  public SQLQuery(final String iText) {
    text = iText.trim();
  }

  /**
   * Delegates to the OQueryExecutor the query execution.
   */
  @SuppressWarnings("unchecked")
  public List<T> run(final Object... iArgs) {
    final DatabaseSessionInternal database = DatabaseRecordThreadLocal.instance().get();
    if (database == null) {
      throw new QueryParsingException("No database configured");
    }

    database.getMetadata().makeThreadLocalSchemaSnapshot();
    try {
      setParameters(iArgs);
      Object o = database.getStorage().command(database, this);
      if (o instanceof List) {
        return (List<T>) o;
      } else {
        return (List<T>) Collections.singletonList(o);
      }

    } finally {
      database.getMetadata().clearThreadLocalSchemaSnapshot();
    }
  }

  /**
   * Returns only the first record if any.
   */
  public T runFirst(DatabaseSessionInternal database, final Object... iArgs) {
    setLimit(1);
    final List<T> result = execute(database, iArgs);
    return result != null && !result.isEmpty() ? result.getFirst() : null;
  }

  public String getText() {
    return text;
  }

  public CommandRequestText setText(final String iText) {
    text = iText;
    return this;
  }

  @Override
  public String toString() {
    return "sql." + text;
  }

  public CommandRequestText fromStream(DatabaseSessionInternal db, final byte[] iStream,
      RecordSerializerNetwork serializer)
      throws SerializationException {
    final MemoryStream buffer = new MemoryStream(iStream);

    queryFromStream(db, buffer, serializer);

    return this;
  }

  public byte[] toStream(DatabaseSessionInternal db, RecordSerializerNetwork serializer)
      throws SerializationException {
    return queryToStream(db, serializer).toByteArray();
  }

  protected MemoryStream queryToStream(DatabaseSessionInternal db,
      RecordSerializerNetwork serializer) {
    final MemoryStream buffer = new MemoryStream();

    buffer.setUtf8(text); // TEXT AS STRING
    buffer.set(limit); // LIMIT AS INTEGER
    buffer.setUtf8(fetchPlan != null ? fetchPlan : "");

    buffer.set(serializeQueryParameters(db, serializer, parameters));

    return buffer;
  }

  protected void queryFromStream(DatabaseSessionInternal db, final MemoryStream buffer,
      RecordSerializerNetwork serializer) {
    text = buffer.getAsString();
    limit = buffer.getAsInteger();

    setFetchPlan(buffer.getAsString());

    final byte[] paramBuffer = buffer.getAsByteArray();
    parameters = deserializeQueryParameters(db, paramBuffer, serializer);
  }

  protected static Map<Object, Object> deserializeQueryParameters(
      DatabaseSessionInternal db, final byte[] paramBuffer, RecordSerializerNetwork serializer) {
    if (paramBuffer == null || paramBuffer.length == 0) {
      return Collections.emptyMap();
    }

    @SuppressWarnings("unchecked")
    var params = (Map<String, ?>) serializer.deserializeValue(db, paramBuffer,
        PropertyType.EMBEDDEDMAP);

    final Map<Object, Object> result = new HashMap<>();
    for (Entry<String, ?> p : params.entrySet()) {
      if (Character.isDigit(p.getKey().charAt(0))) {
        result.put(Integer.parseInt(p.getKey()), p.getValue());
      } else {
        result.put(p.getKey(), p.getValue());
      }
    }

    return result;
  }

  protected static byte[] serializeQueryParameters(DatabaseSessionInternal db,
      RecordSerializerNetwork serializer, final Map<Object, Object> params) {
    if (params == null || params.isEmpty())
    // NO PARAMETER, JUST SEND 0
    {
      return CommonConst.EMPTY_BYTE_ARRAY;
    }

    return serializer.serializeValue(db, convertToRIDsIfPossible(params),
        PropertyType.EMBEDDEDMAP);
  }

  private static Map<Object, Object> convertToRIDsIfPossible(final Map<Object, Object> params) {
    final Map<Object, Object> newParams = new HashMap<>(params.size());

    for (Entry<Object, Object> entry : params.entrySet()) {
      final Object value = entry.getValue();

      switch (value) {
        case Set<?> objects when !objects.isEmpty() && objects.iterator()
            .next() instanceof Record -> {
          // CONVERT RECORDS AS RIDS
          final Set<RID> newSet = new HashSet<>();
          //noinspection unchecked
          for (Record rec : (Set<Record>) value) {
            newSet.add(rec.getIdentity());
          }
          newParams.put(entry.getKey(), newSet);
        }
        case List<?> objects when !objects.isEmpty() && objects.getFirst() instanceof Record -> {
          // CONVERT RECORDS AS RIDS
          final List<RID> newList = new ArrayList<>();
          //noinspection unchecked
          for (Record rec : (List<Record>) value) {
            newList.add(rec.getIdentity());
          }
          newParams.put(entry.getKey(), newList);
        }
        case Map<?, ?> map when !map.isEmpty() && map.values().iterator()
            .next() instanceof Record -> {
          // CONVERT RECORDS AS RIDS
          final Map<Object, RID> newMap = new HashMap<>();
          //noinspection unchecked
          for (Entry<?, Record> mapEntry : ((Map<?, Record>) value).entrySet()) {
            newMap.put(mapEntry.getKey(), mapEntry.getValue().getIdentity());
          }
          newParams.put(entry.getKey(), newMap);
        }
        case Identifiable identifiable -> newParams.put(entry.getKey(), identifiable.getIdentity());
        case null, default -> newParams.put(entry.getKey(), value);
      }
    }

    return newParams;
  }
}
