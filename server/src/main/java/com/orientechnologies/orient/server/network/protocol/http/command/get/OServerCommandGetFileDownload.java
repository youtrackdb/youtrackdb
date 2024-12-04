/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.server.network.protocol.http.command.get;

import com.orientechnologies.common.util.OPatternConst;
import com.orientechnologies.orient.core.exception.ORecordNotFoundException;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTRecordAbstract;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.record.impl.ODocumentInternal;
import com.orientechnologies.orient.server.network.protocol.http.OHttpRequest;
import com.orientechnologies.orient.server.network.protocol.http.OHttpResponse;
import com.orientechnologies.orient.server.network.protocol.http.OHttpUtils;
import com.orientechnologies.orient.server.network.protocol.http.command.OServerCommandAuthenticatedDbAbstract;
import java.io.IOException;
import java.util.Date;

/**
 *
 */
public class OServerCommandGetFileDownload extends OServerCommandAuthenticatedDbAbstract {

  private static final String[] NAMES = {"GET|fileDownload/*"};

  @Override
  public boolean execute(OHttpRequest iRequest, OHttpResponse iResponse) throws Exception {
    String[] urlParts =
        checkSyntax(
            iRequest.getUrl(),
            3,
            "Syntax error: fileDownload/<database>/rid/[/<fileName>][/<fileType>].");

    final String fileName = urlParts.length > 3 ? encodeResponseText(urlParts[3]) : "unknown";

    final String fileType;
    if (urlParts.length > 5) {
      fileType = encodeResponseText(urlParts[4]) + '/' + encodeResponseText(urlParts[5]);
    } else {
      fileType = (urlParts.length > 4 ? encodeResponseText(urlParts[4]) : "");
    }

    final String rid = urlParts[2];

    iRequest.getData().commandInfo = "Download";
    iRequest.getData().commandDetail = rid;

    final YTRecordAbstract response;
    var db = getProfiledDatabaseInstance(iRequest);
    try {
      try {
        response = db.load(new YTRecordId(rid));
        if (response instanceof YTBlob) {
          sendORecordBinaryFileContent(
              iResponse,
              OHttpUtils.STATUS_OK_CODE,
              OHttpUtils.STATUS_OK_DESCRIPTION,
              fileType,
              (YTBlob) response,
              fileName);
        } else if (response instanceof YTDocument) {
          for (YTProperty prop :
              ODocumentInternal.getImmutableSchemaClass(((YTDocument) response)).properties(db)) {
            if (prop.getType().equals(YTType.BINARY)) {
              sendBinaryFieldFileContent(
                  iRequest,
                  iResponse,
                  OHttpUtils.STATUS_OK_CODE,
                  OHttpUtils.STATUS_OK_DESCRIPTION,
                  fileType,
                  ((YTDocument) response).field(prop.getName()),
                  fileName);
            }
          }
        }
      } catch (ORecordNotFoundException rnf) {
        iResponse.send(
            OHttpUtils.STATUS_INVALIDMETHOD_CODE,
            "Record requested is not a file nor has a readable schema",
            OHttpUtils.CONTENT_TEXT_PLAIN,
            "Record requested is not a file nor has a readable schema",
            null);
      }
    } catch (Exception e) {
      iResponse.send(
          OHttpUtils.STATUS_INTERNALERROR_CODE,
          OHttpUtils.STATUS_INTERNALERROR_DESCRIPTION,
          OHttpUtils.CONTENT_TEXT_PLAIN,
          e.getMessage(),
          null);
    } finally {
      if (db != null) {
        db.close();
      }
    }

    return false;
  }

  protected static void sendORecordBinaryFileContent(
      final OHttpResponse iResponse,
      final int iCode,
      final String iReason,
      final String iContentType,
      final YTBlob record,
      final String iFileName)
      throws IOException {
    iResponse.writeStatus(iCode, iReason);
    iResponse.writeHeaders(iContentType);
    iResponse.writeLine("Content-Disposition: attachment; filename=" + iFileName);
    iResponse.writeLine("Date: " + new Date());
    iResponse.writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (((YTRecordAbstract) record).getSize()));
    iResponse.writeLine(null);

    record.toOutputStream(iResponse.getOutputStream());

    iResponse.flush();
  }

  protected void sendBinaryFieldFileContent(
      final OHttpRequest iRequest,
      final OHttpResponse iResponse,
      final int iCode,
      final String iReason,
      final String iContentType,
      final byte[] record,
      final String iFileName)
      throws IOException {
    iResponse.writeStatus(iCode, iReason);
    iResponse.writeHeaders(iContentType);
    iResponse.writeLine("Content-Disposition: attachment; filename=" + iFileName);
    iResponse.writeLine("Date: " + new Date());
    iResponse.writeLine(OHttpUtils.HEADER_CONTENT_LENGTH + (record.length));
    iResponse.writeLine(null);

    iResponse.getOutputStream().write(record);

    iResponse.flush();
  }

  @Override
  public String[] getNames() {
    return NAMES;
  }

  private static String encodeResponseText(String iText) {
    iText = OPatternConst.PATTERN_SINGLE_SPACE.matcher(iText).replaceAll("%20");
    iText = OPatternConst.PATTERN_AMP.matcher(iText).replaceAll("%26");
    return iText;
  }
}
