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
package com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.get;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpUtils;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.ServerCommandDocumentAbstract;
import java.util.Iterator;
import java.util.stream.Stream;

public class ServerCommandGetIndex extends ServerCommandDocumentAbstract {

  private static final String[] NAMES = {"GET|index/*"};

  @SuppressWarnings("unchecked")
  @Override
  public boolean execute(final OHttpRequest iRequest, HttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: index/<database>/<index-name>/<key>");

    iRequest.getData().commandInfo = "Index get";

    DatabaseSessionInternal db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, urlParts[2]);
      if (index == null) {
        throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");
      }

      try (final Stream<RID> stream = index.getInternal().getRids(db, urlParts[3])) {
        final Iterator<RID> iterator = stream.iterator();

        if (!iterator.hasNext()) {
          iResponse.send(
              HttpUtils.STATUS_NOTFOUND_CODE,
              HttpUtils.STATUS_NOTFOUND_DESCRIPTION,
              HttpUtils.CONTENT_TEXT_PLAIN,
              null,
              null);
        } else {
          final StringBuilder buffer = new StringBuilder(128);
          buffer.append('[');

          int count = 0;
          while (iterator.hasNext()) {
            final RID item = iterator.next();
            if (count > 0) {
              buffer.append(", ");
            }
            buffer.append(item.getRecord().toJSON());
            count++;
          }

          buffer.append(']');

          if (isJsonResponse(iResponse)) {
            iResponse.send(
                HttpUtils.STATUS_OK_CODE,
                HttpUtils.STATUS_OK_DESCRIPTION,
                HttpUtils.CONTENT_JSON,
                buffer.toString(),
                null);
          } else {
            iResponse.send(
                HttpUtils.STATUS_OK_CODE,
                HttpUtils.STATUS_OK_DESCRIPTION,
                HttpUtils.CONTENT_TEXT_PLAIN,
                buffer.toString(),
                null);
          }
        }
      }
    } finally {
      if (db != null) {
        db.close();
      }
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
