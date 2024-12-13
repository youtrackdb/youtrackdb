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
package com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.all;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.config.OServerCommandConfiguration;
import java.io.IOException;

public class ServerCommandFunction extends ServerCommandAbstractLogic {

  private static final String[] NAMES = {"GET|function/*", "POST|function/*"};

  public ServerCommandFunction() {
  }

  public ServerCommandFunction(final OServerCommandConfiguration iConfig) {
  }

  @Override
  public String[] init(final OHttpRequest iRequest, final HttpResponse iResponse) {
    final String[] parts =
        checkSyntax(iRequest.getUrl(), 3, "Syntax error: function/<database>/<name>[/param]*");
    iRequest.getData().commandInfo = "Execute a function";
    return parts;
  }

  @Override
  protected void handleResult(
      final OHttpRequest iRequest,
      final HttpResponse iResponse,
      final Object iResult,
      DatabaseSessionInternal databaseDocumentInternal)
      throws InterruptedException, IOException {
    iResponse.writeResult(iResult, databaseDocumentInternal);
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }
}
