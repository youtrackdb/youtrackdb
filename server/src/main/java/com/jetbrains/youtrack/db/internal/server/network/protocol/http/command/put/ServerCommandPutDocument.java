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

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.internal.core.id.ChangeableRecordId;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpUtils;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.ServerCommandDocumentAbstract;

public class ServerCommandPutDocument extends ServerCommandDocumentAbstract {

  private static final String[] NAMES = {"PUT|document/*"};

  @Override
  public boolean execute(final HttpRequest iRequest, HttpResponse iResponse) throws Exception {
    final var urlParts =
        checkSyntax(
            iRequest.getUrl(),
            2,
            "Syntax error: document/<database>[/<record-id>][?updateMode=full|partial]");

    iRequest.getData().commandInfo = "Edit Document";

    try (var db = getProfiledDatabaseSessionInstance(iRequest)) {
      RecordId recordId;
      if (urlParts.length > 2) {
        // EXTRACT RID
        final var parametersPos = urlParts[2].indexOf('?');
        final var rid =
            parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
        recordId = new RecordId(rid);

        if (!recordId.isValid()) {
          throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
        }
      } else {
        recordId = new ChangeableRecordId();
      }

      var d =
          db.computeInTx(
              () -> {
                var txRecordId = recordId;
                final EntityImpl entity;
                // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
                entity = new EntityImpl(db);
                entity.updateFromJSON(iRequest.getContent());
                entity.setTrackingChanges(false);

                if (iRequest.getIfMatch() != null)
                // USE THE IF-MATCH HTTP HEADER AS VERSION
                {
                  RecordInternal.setVersion(entity, Integer.parseInt(iRequest.getIfMatch()));
                }

                if (!txRecordId.isValid()) {
                  txRecordId = entity.getIdentity();
                }

                if (!txRecordId.isValid()) {
                  throw new IllegalArgumentException("Invalid Record ID in request: " + txRecordId);
                }

                final EntityImpl currentEntity;
                try {
                  currentEntity = db.load(txRecordId);
                } catch (RecordNotFoundException rnf) {
                  return null;
                }

                var partialUpdateMode = false;
                var mode = iRequest.getParameter("updateMode");
                if (mode != null && mode.equalsIgnoreCase("partial")) {
                  partialUpdateMode = true;
                }

                mode = iRequest.getHeader("updateMode");
                if (mode != null && mode.equalsIgnoreCase("partial")) {
                  partialUpdateMode = true;
                }

                currentEntity.merge(entity, partialUpdateMode, false);
                if (currentEntity.isDirty()) {
                  if (entity.getVersion() > 0)
                  // OVERWRITE THE VERSION
                  {
                    RecordInternal.setVersion(currentEntity, entity.getVersion());
                  }

                }

                return currentEntity;
              });

      if (d == null) {
        iResponse.send(
            HttpUtils.STATUS_NOTFOUND_CODE,
            HttpUtils.STATUS_NOTFOUND_DESCRIPTION,
            HttpUtils.CONTENT_TEXT_PLAIN,
            "Record " + recordId + " was not found.",
            null);
        return false;
      }

      d = db.bindToSession(d);
      iResponse.send(
          HttpUtils.STATUS_OK_CODE,
          HttpUtils.STATUS_OK_DESCRIPTION,
          HttpUtils.CONTENT_JSON,
          d.toJSON(),
          HttpUtils.HEADER_ETAG + d.getVersion());
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
