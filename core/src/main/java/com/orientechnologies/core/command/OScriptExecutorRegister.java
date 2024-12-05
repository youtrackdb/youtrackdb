package com.orientechnologies.core.command;

import com.orientechnologies.core.command.script.OScriptManager;

/**
 *
 */
public interface OScriptExecutorRegister {

  void registerExecutor(OScriptManager scriptManager, OCommandManager commandManager);
}
