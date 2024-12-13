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

import com.jetbrains.youtrack.db.api.exception.RecordNotFoundException;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.ServerCommandAuthenticatedDbAbstract;

public class ServerCommandGetDictionary extends ServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = {"GET|dictionary/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, HttpResponse iResponse) throws Exception {
    iRequest.getData().commandInfo = "Dictionary lookup";

    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: dictionary/<database>/<key>");

    DatabaseSessionInternal db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      final Record record = db.getDictionary().get(urlParts[2]);
      if (record == null) {
        throw new RecordNotFoundException(
            null, "Key '" + urlParts[2] + "' was not found in the database dictionary");
      }

      iResponse.writeRecord(record);

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
