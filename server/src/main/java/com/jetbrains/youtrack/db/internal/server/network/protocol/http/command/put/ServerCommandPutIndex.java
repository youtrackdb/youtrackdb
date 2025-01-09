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
package com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.put;

import com.jetbrains.youtrack.db.api.record.DBRecord;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.index.IndexDefinition;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpUtils;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.ServerCommandDocumentAbstract;

public class ServerCommandPutIndex extends ServerCommandDocumentAbstract {

  private static final String[] NAMES = {"PUT|index/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, HttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(
            iRequest.getUrl(), 3, "Syntax error: index/<database>/<index-name>/<key>[/<value>]");

    iRequest.getData().commandInfo = "Index put";

    DatabaseSessionInternal db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, urlParts[2]);
      if (index == null) {
        throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");
      }

      final Identifiable record;

      if (urlParts.length > 4)
      // GET THE RECORD ID AS VALUE
      {
        record = new RecordId(urlParts[4]);
      } else {
        // GET THE REQUEST CONTENT AS DOCUMENT
        if (iRequest.getContent() == null || iRequest.getContent().isEmpty()) {
          throw new IllegalArgumentException("Index's entry value is null");
        }

        var entity = new EntityImpl();
        entity.fromJSON(iRequest.getContent());
        record = entity;
      }

      final IndexDefinition indexDefinition = index.getDefinition();
      final Object key;
      if (indexDefinition != null) {
        key = indexDefinition.createValue(db, urlParts[3]);
      } else {
        key = urlParts[3];
      }

      if (key == null) {
        throw new IllegalArgumentException("Invalid key value : " + urlParts[3]);
      }

      final boolean existent = record.getIdentity().isPersistent();

      if (existent && record instanceof DBRecord) {
        ((DBRecord) record).save();
      }

      index.put(db, key, record);

      if (existent) {
        iResponse.send(
            HttpUtils.STATUS_OK_CODE,
            HttpUtils.STATUS_OK_DESCRIPTION,
            HttpUtils.CONTENT_TEXT_PLAIN,
            null,
            null);
      } else {
        iResponse.send(
            HttpUtils.STATUS_CREATED_CODE,
            HttpUtils.STATUS_CREATED_DESCRIPTION,
            HttpUtils.CONTENT_TEXT_PLAIN,
            null,
            null);
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
