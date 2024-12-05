package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.OCommandContext;

public interface OTemporaryRidGenerator {

  int getTemporaryRIDCounter(final OCommandContext iContext);
}
