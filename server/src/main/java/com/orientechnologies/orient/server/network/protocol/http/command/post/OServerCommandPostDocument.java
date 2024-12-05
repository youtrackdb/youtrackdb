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
package com.orientechnologies.orient.server.network.protocol.http.command.post;

import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.record.ORecordInternal;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPostDocument extends OServerCommandDocumentAbstract {

  private static final String[] NAMES = {"POST|document/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    checkSyntax(iRequest.getUrl(), 2, "Syntax error: document/<database>");

    iRequest.getData().commandInfo = "Create document";

    YTDatabaseSession db = null;

    YTEntityImpl d;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      d =
          db.computeInTx(
              () -> {
                YTEntityImpl doc = new YTEntityImpl();
                doc.fromJSON(iRequest.getContent());
                ORecordInternal.setVersion(doc, 0);

                // ASSURE TO MAKE THE RECORD ID INVALID
                ((YTRecordId) doc.getIdentity()).setClusterPosition(YTRID.CLUSTER_POS_INVALID);

                doc.save();
                return doc;
              });

      d = db.bindToSession(d);
      iResponse.send(
          OHttpUtils.STATUS_CREATED_CODE,
          OHttpUtils.STATUS_CREATED_DESCRIPTION,
          OHttpUtils.CONTENT_JSON,
          d.toJSON(),
          OHttpUtils.HEADER_ETAG + d.getVersion());

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
