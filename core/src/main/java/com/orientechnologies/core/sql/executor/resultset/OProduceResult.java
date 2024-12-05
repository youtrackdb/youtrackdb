package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;

public interface OProduceResult {

  YTResult produce(OCommandContext ctx);
}
