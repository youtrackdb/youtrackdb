package com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.get;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.JSONWriter;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpResponse;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.HttpUtils;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.OHttpRequest;
import com.jetbrains.youtrack.db.internal.server.network.protocol.http.command.ServerCommandAbstract;
import java.io.StringWriter;

public class ServerCommandGetSSO extends ServerCommandAbstract {

  private static final String[] NAMES = {"GET|sso"};

  @Override
  public String[] getNames() {
    return NAMES;
  }

  public ServerCommandGetSSO() {
  }

  @Override
  public boolean execute(final OHttpRequest iRequest, final HttpResponse iResponse)
      throws Exception {
    getJSON(iResponse);

    return false; // Is not a chained command.
  }

  private void getJSON(final HttpResponse iResponse) {
    try {
      final StringWriter buffer = new StringWriter();
      final JSONWriter json = new JSONWriter(buffer, HttpResponse.JSON_FORMAT);

      json.beginObject();

      json.writeAttribute("enabled", getServer().getSecurity().isSingleSignOnSupported());

      json.endObject();

      iResponse.send(
          HttpUtils.STATUS_OK_CODE,
          HttpUtils.STATUS_OK_DESCRIPTION,
          HttpUtils.CONTENT_JSON,
          buffer.toString(),
          null);
    } catch (Exception ex) {
      LogManager.instance().error(this, "ServerCommandGetSSO.getJSON() Exception: %s", ex);
    }
  }
}
