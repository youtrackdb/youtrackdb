package com.orientechnologies.core.sql.executor.resultset;

import com.orientechnologies.core.command.OCommandContext;
import com.orientechnologies.core.sql.executor.YTResult;

public interface OResultMapper {

  YTResult map(YTResult result, OCommandContext ctx);
}
