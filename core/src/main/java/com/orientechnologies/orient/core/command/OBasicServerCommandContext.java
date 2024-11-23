package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OxygenDBInternal;

public class OBasicServerCommandContext extends OBasicCommandContext
    implements OServerCommandContext {

  private OxygenDBInternal server;

  public OBasicServerCommandContext() {
  }

  public OBasicServerCommandContext(OxygenDBInternal server) {
    this.server = server;
  }

  public OxygenDBInternal getServer() {
    return server;
  }

  public void setServer(OxygenDBInternal server) {
    this.server = server;
  }
}
