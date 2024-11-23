package com.orientechnologies.orient.core.sql.executor;

import java.io.Serializable;
import java.util.List;

/**
 *
 */
public interface OExecutionPlan extends Serializable {

  List<OExecutionStep> getSteps();

  String prettyPrint(int depth, int indent);

  OResult toResult();
}
