package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.command.script.OScriptManager;

/**
 *
 */
public interface OScriptExecutorRegister {

  void registerExecutor(OScriptManager scriptManager, OCommandManager commandManager);
}
