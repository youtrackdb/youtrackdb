package com.orientechnologies.core.command;

import com.orientechnologies.core.db.YouTrackDBInternal;

public interface OServerCommandContext extends OCommandContext {

  YouTrackDBInternal getServer();
}
