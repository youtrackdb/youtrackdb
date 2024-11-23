package com.orientechnologies.orient.core.command;

import com.orientechnologies.orient.core.command.script.OScriptManager;

/**
 *
 */
public interface OScriptExecutorRegister {

  void registerExecutor(OScriptManager scriptManager, OCommandManager commandManager);
}
