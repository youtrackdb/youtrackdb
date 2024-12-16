package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;

public interface TemporaryRidGenerator {

  int getTemporaryRIDCounter(final CommandContext iContext);
}
