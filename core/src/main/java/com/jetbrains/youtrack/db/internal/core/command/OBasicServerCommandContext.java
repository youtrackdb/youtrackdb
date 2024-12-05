package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;

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
