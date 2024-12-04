package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.YouTrackDBInternal;

public class OBasicServerCommandContext extends OBasicCommandContext
    implements OServerCommandContext {

  private YouTrackDBInternal server;

  public OBasicServerCommandContext() {
  }

  public OBasicServerCommandContext(YouTrackDBInternal server) {
    this.server = server;
  }

  public YouTrackDBInternal getServer() {
    return server;
  }

  public void setServer(YouTrackDBInternal server) {
    this.server = server;
  }
}
