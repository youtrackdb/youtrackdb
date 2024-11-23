package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.db.OxygenDBInternal;

public interface OServerCommandContext extends OCommandContext {

  OxygenDBInternal getServer();
}
