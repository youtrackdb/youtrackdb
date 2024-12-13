/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.jetbrains.youtrack.db.internal.core.command.script.ScriptManager;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.util.HashSet;
import java.util.Set;

public class OServerCommandGetSupportedLanguages extends OServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = {"GET|supportedLanguages/*"};

  @Override
  public boolean execute(final OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts =
        checkSyntax(iRequest.getUrl(), 2, "Syntax error: supportedLanguages/<database>");

    iRequest.getData().commandInfo = "Returns the supported languages";

    DatabaseSession db = null;

    try {
      db = getProfiledDatabaseInstance(iRequest);

      EntityImpl result = new EntityImpl();
      Set<String> languages = new HashSet<String>();

      ScriptManager scriptManager =
          YouTrackDBInternal.extract(server.getContext()).getScriptManager();
      for (String language : scriptManager.getSupportedLanguages()) {
        if (scriptManager.getFormatters() != null
            && scriptManager.getFormatters().get(language) != null) {
          languages.add(language);
        }
      }

      result.field("languages", languages);
      iResponse.writeRecord(result);
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
