package com.jetbrains.youtrack.db.internal.core.command;

import com.jetbrains.youtrack.db.internal.core.command.script.ScriptManager;

/**
 *
 */
public interface ScriptExecutorRegister {

  void registerExecutor(ScriptManager scriptManager, CommandManager commandManager);
}
