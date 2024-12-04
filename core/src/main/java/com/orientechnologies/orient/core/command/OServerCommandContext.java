package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.YouTrackDBInternal;

public interface OServerCommandContext extends OCommandContext {

  YouTrackDBInternal getServer();
}
