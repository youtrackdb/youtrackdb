package com.orientechnologies.orient.core.sql.executor.resultset;

import com.orientechnologies.orient.core.command.OCommandContext;
import com.orientechnologies.orient.core.sql.executor.YTResult;

public interface OMapExecutionStream {

  OExecutionStream flatMap(YTResult next, OCommandContext ctx);
}
