package com.jetbrains.youtrack.db.internal.core.sql.executor;

/**
 *
 */
public class ExecutionPlanPrintUtils {

  public static void printExecutionPlan(YTResultSet result) {
    printExecutionPlan(null, result);
  }

  public static void printExecutionPlan(String query, YTResultSet result) {
    //    if (query != null) {
    //      System.out.println(query);
    //    }
    //    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    //    System.out.println();
  }
}
