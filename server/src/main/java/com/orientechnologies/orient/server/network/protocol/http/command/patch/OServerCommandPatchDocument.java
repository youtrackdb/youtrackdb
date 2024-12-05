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
package com.orientechnologies.orient.server.network.protocol.http.command.patch;

import com.orientechnologies.common.util.ORawPair;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.exception.YTRecordNotFoundException;
import com.orientechnologies.orient.core.id.ChangeableRecordId;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPatchDocument extends OServerCommandDocumentAbstract {

  private static final String[] NAMES = {"PATCH|document/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: document/<database>[/<record-id>]");

    iRequest.getData().commandInfo = "Edit Document";
    try (YTDatabaseSession db = getProfiledDatabaseInstance(iRequest)) {
      ORawPair<Boolean, YTRID> result =
          db.computeInTx(
              () -> {
                YTRecordId recordId;

                if (urlParts.length > 2) {
                  // EXTRACT RID
                  final int parametersPos = urlParts[2].indexOf('?');
                  final String rid =
                      parametersPos > -1 ? urlParts[2].substring(0, parametersPos) : urlParts[2];
                  recordId = new YTRecordId(rid);

                  if (!recordId.isValid()) {
                    throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
                  }
                } else {
                  recordId = new ChangeableRecordId();
                }

                // UNMARSHALL DOCUMENT WITH REQUEST CONTENT
                var doc = new YTEntityImpl();
                doc.fromJSON(iRequest.getContent());

                if (iRequest.getIfMatch() != null)
                // USE THE IF-MATCH HTTP HEADER AS VERSION
                {
                  ORecordInternal.setVersion(doc, Integer.parseInt(iRequest.getIfMatch()));
                }

                if (!recordId.isValid()) {
                  recordId = (YTRecordId) doc.getIdentity();
                } else {
                  ORecordInternal.setIdentity(doc, recordId);
                }

                if (!recordId.isValid()) {
                  throw new IllegalArgumentException("Invalid Record ID in request: " + recordId);
                }

                final YTEntityImpl currentDocument;

                try {
                  currentDocument = db.load(recordId);
                } catch (YTRecordNotFoundException rnf) {
                  return new ORawPair<>(false, recordId);
                }

                boolean partialUpdateMode = true;
                currentDocument.merge(doc, partialUpdateMode, false);
                ORecordInternal.setVersion(currentDocument, doc.getVersion());

                currentDocument.save();
                return new ORawPair<>(true, recordId);
              });

      if (!result.first) {
        iResponse.send(
            OHttpUtils.STATUS_NOTFOUND_CODE,
            OHttpUtils.STATUS_NOTFOUND_DESCRIPTION,
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record " + result.second + " was not found.",
            null);
        return false;
      }

      var record = db.load(result.second);
      iResponse.send(
          OHttpUtils.STATUS_OK_CODE,
          OHttpUtils.STATUS_OK_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          record.toJSON(),
          OHttpUtils.HEADER_ETAG + record.getVersion());
    }
    return false;
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
