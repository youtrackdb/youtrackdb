package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.CommandContext;

public interface OTemporaryRidGenerator {

  int getTemporaryRIDCounter(final CommandContext iContext);
}
