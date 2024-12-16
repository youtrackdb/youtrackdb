package com.jetbrains.youtrack.db.internal.core.command;

public interface DynamicVariable {

  Object resolve(CommandContext contex);
}
