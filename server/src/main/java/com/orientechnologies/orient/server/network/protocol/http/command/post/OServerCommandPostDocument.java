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

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.id.RID;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.RecordInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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

    DatabaseSession db = null;

    EntityImpl d;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      d =
          db.computeInTx(
              () -> {
                EntityImpl entity = new EntityImpl();
                entity.fromJSON(iRequest.getContent());
                RecordInternal.setVersion(entity, 0);

                // ASSURE TO MAKE THE RECORD ID INVALID
                ((RecordId) entity.getIdentity()).setClusterPosition(RID.CLUSTER_POS_INVALID);

                entity.save();
                return entity;
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
