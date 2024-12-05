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
package com.orientechnologies.orient.server.network.protocol.http.command.put;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandDocumentAbstract;

public class OServerCommandPutIndex extends OServerCommandDocumentAbstract {

  private static final String[] NAMES = {"PUT|index/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    final String[] urlParts =
        checkSyntax(
            iRequest.getUrl(), 3, "Syntax error: index/<database>/<index-name>/<key>[/<value>]");

    iRequest.getData().commandInfo = "Index put";

    YTDatabaseSessionInternal db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, urlParts[2]);
      if (index == null) {
        throw new IllegalArgumentException("Index name '" + urlParts[2] + "' not found");
      }

      final YTIdentifiable record;

      if (urlParts.length > 4)
      // GET THE RECORD ID AS VALUE
      {
        record = new YTRecordId(urlParts[4]);
      } else {
        // GET THE REQUEST CONTENT AS DOCUMENT
        if (iRequest.getContent() == null || iRequest.getContent().isEmpty()) {
          throw new IllegalArgumentException("Index's entry value is null");
        }

        var doc = new EntityImpl();
        doc.fromJSON(iRequest.getContent());
        record = doc;
      }

      final OIndexDefinition indexDefinition = index.getDefinition();
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

      if (existent && record instanceof Record) {
        ((Record) record).save();
      }

      index.put(db, key, record);

      if (existent) {
        iResponse.send(
            OHttpUtils.STATUS_OK_CODE,
            OHttpUtils.STATUS_OK_DESCRIPTION,
            OHttpUtils.CONTENT_TEXT_PLAIN,
            null,
            null);
      } else {
        iResponse.send(
            OHttpUtils.STATUS_CREATED_CODE,
            OHttpUtils.STATUS_CREATED_DESCRIPTION,
            OHttpUtils.CONTENT_TEXT_PLAIN,
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
