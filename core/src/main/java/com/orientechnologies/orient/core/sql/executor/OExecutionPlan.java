package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import java.io.Serializable;
import java.util.List;

/**
 *
 */
public interface OExecutionPlan extends Serializable {

  List<OExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  OResult toResult(ODatabaseSessionInternal db);
}
