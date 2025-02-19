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
import com.jetbrains.youtrack.db.internal.core.command.CommandResultListener;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.serialization.MemoryStream;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.record.binary.RecordSerializerNetwork;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nonnull;

/**
 * SQL synchronous query. When executed the caller wait for the result.
 *
 * @param <T>
 * @see SQLAsynchQuery
 */
@SuppressWarnings({"unchecked", "serial"})
public class SQLSynchQuery<T extends Object> extends SQLAsynchQuery<T>
    implements CommandResultListener {

  private final LegacyResultSet<T> result = new ConcurrentLegacyResultSet<T>();
  private RID nextPageRID;
  private Map<Object, Object> previousQueryParams = new HashMap<Object, Object>();

  public SQLSynchQuery() {
    resultListener = this;
  }

  public SQLSynchQuery(final String iText) {
    super(iText);
    resultListener = this;
  }

  public SQLSynchQuery(final String iText, final int iLimit) {
    super(iText, iLimit, null);
    resultListener = this;
  }

  @Override
  public void reset() {
    result.clear();
  }

  public boolean result(@Nonnull DatabaseSessionInternal session, final Object iRecord) {
    if (iRecord != null) {
      result.add((T) iRecord);
    }
    return true;
  }

  @Override
  public void end(@Nonnull DatabaseSessionInternal session) {
    result.setCompleted();
  }

  @Override
  public List<T> run(DatabaseSessionInternal session, final Object... iArgs) {
    result.clear();

    final Map<Object, Object> queryParams;
    queryParams = fetchQueryParams(iArgs);
    resetNextRIDIfParametersWereChanged(queryParams);

    final var res = (List<Object>) super.run(session, iArgs);

    if (res != result && res != null && result.isEmptyNoWait()) {
      var iter = res.iterator();
      while (iter.hasNext()) {
        var item = iter.next();
        result.add((T) item);
      }
    }

    ((LegacyResultSet) result).setCompleted();

    if (!result.isEmpty()) {
      previousQueryParams = new HashMap<>(queryParams);
      final var lastRid = (RecordId) ((Identifiable) result.get(
          result.size() - 1)).getIdentity();
      nextPageRID = new RecordId(lastRid.next());
    }

    return result;
  }

  @Override
  public boolean isIdempotent() {
    return true;
  }

  public Object getResult() {
    return result;
  }

  /**
   * @return RID of the record that will be processed first during pagination mode.
   */
  public RID getNextPageRID() {
    return nextPageRID;
  }

  public void resetPagination() {
    nextPageRID = null;
  }

  @Override
  public boolean isAsynchronous() {
    return false;
  }

  @Override
  protected MemoryStream queryToStream(DatabaseSessionInternal db,
      RecordSerializerNetwork serializer) {
    final var buffer = super.queryToStream(db, serializer);

    buffer.setUtf8(nextPageRID != null ? nextPageRID.toString() : "");

    final var queryParams = serializeQueryParameters(db, serializer, previousQueryParams);
    buffer.set(queryParams);

    return buffer;
  }

  @Override
  protected void queryFromStream(DatabaseSessionInternal db, final MemoryStream buffer,
      RecordSerializerNetwork serializer) {
    super.queryFromStream(db, buffer, serializer);

    final var rid = buffer.getAsString();
    if ("".equals(rid)) {
      nextPageRID = null;
    } else {
      nextPageRID = new RecordId(rid);
    }

    final var serializedPrevParams = buffer.getAsByteArray();
    previousQueryParams = deserializeQueryParameters(db, serializedPrevParams, serializer);
  }

  private void resetNextRIDIfParametersWereChanged(final Map<Object, Object> queryParams) {
    if (!queryParams.equals(previousQueryParams)) {
      nextPageRID = null;
    }
  }

  private Map<Object, Object> fetchQueryParams(final Object... iArgs) {
    if (iArgs != null && iArgs.length > 0) {
      return convertToParameters(iArgs);
    }

    var queryParams = getParameters();
    if (queryParams == null) {
      queryParams = new HashMap<Object, Object>();
    }
    return queryParams;
  }
}
