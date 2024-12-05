package com.orientechnologies.core.sql;

import com.orientechnologies.core.command.OCommandContext;

public interface OTemporaryRidGenerator {

  int getTemporaryRIDCounter(final OCommandContext iContext);
}
