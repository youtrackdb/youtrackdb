package com.orientechnologies.core.sql.executor;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
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
