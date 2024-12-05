package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public interface OExecutionPlan extends Serializable {

  List<OExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  YTResult toResult(YTDatabaseSessionInternal db);
}
