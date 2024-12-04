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
package com.orientechnologies.orient.server.network.protocol.http.command;

import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.serialization.serializer.OStringSerializerHelper;
import com.orientechnologies.orient.server.OServer;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.YTHttpRequestException;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponseAbstract;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import java.io.IOException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public abstract class OServerCommandAbstract implements OServerCommand {

  protected OServer server;

  /**
   * Default constructor. Disable cache of content at HTTP level
   */
  public OServerCommandAbstract() {
  }

  @Override
  public boolean beforeExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    setNoCache(iResponse);
    return true;
  }

  @Override
  public boolean afterExecute(final OHttpRequest iRequest, OHttpResponse iResponse)
      throws IOException {
    return true;
  }

  protected String[] checkSyntax(
      final String iURL, final int iArgumentCount, final String iSyntax) {
    final List<String> parts =
        OStringSerializerHelper.smartSplit(
            iURL, OHttpResponseAbstract.URL_SEPARATOR, 1, -1, true, true, false, false);
    for (int i = 0; i < parts.size(); i++) {
      parts.set(i, URLDecoder.decode(parts.get(i), StandardCharsets.UTF_8));
    }
    if (parts.size() < iArgumentCount) {
      throw new YTHttpRequestException(iSyntax);
    }

    return parts.toArray(new String[parts.size()]);
  }

  public OServer getServer() {
    return server;
  }

  public void configure(final OServer server) {
    this.server = server;
  }

  protected void setNoCache(final OHttpResponse iResponse) {
    // DEFAULT = DON'T CACHE
    iResponse.setHeader(
        "Cache-Control: no-cache, no-store, max-age=0, must-revalidate\r\nPragma: no-cache");
    iResponse.addHeader("Cache-Control", "no-cache, no-store, max-age=0");
    iResponse.addHeader("Pragma", "no-cache");
  }

  protected boolean isJsonResponse(OHttpResponse response) {
    return response.isJsonErrorResponse();
  }

  protected void sendJsonError(
      OHttpResponse iResponse,
      final int iCode,
      final String iReason,
      final String iContentType,
      final Object iContent,
      final String iHeaders)
      throws IOException {
    YTDocument response = new YTDocument();
    YTDocument error = new YTDocument();
    error.field("code", iCode);
    error.field("reason", iReason);
    error.field("content", iContent);
    List<YTDocument> errors = new ArrayList<YTDocument>();
    errors.add(error);
    response.field("errors", errors);
    iResponse.send(
        iCode, iReason, OHttpUtils.CONTENT_JSON, response.toJSON("prettyPrint"), iHeaders);
  }
}
