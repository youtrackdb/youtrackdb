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
package com.orientechnologies.core.sql;

import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import java.util.Iterator;
import java.util.Map;

/**
 * SQL UPDATE command.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLResultsetDelegate extends OCommandExecutorSQLDelegate
    implements OIterableRecordSource, Iterable<YTIdentifiable> {

  @Override
  public Iterator<YTIdentifiable> iterator() {
    return ((OIterableRecordSource) delegate).iterator(ODatabaseRecordThreadLocal.instance().get(),
        null);
  }

  @Override
  public Iterator<YTIdentifiable> iterator(YTDatabaseSessionInternal querySession,
      final Map<Object, Object> iArgs) {
    return ((OIterableRecordSource) delegate).iterator(querySession, iArgs);
  }
}
