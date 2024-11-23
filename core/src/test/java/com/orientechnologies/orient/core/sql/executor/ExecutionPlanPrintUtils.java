package com.orientechnologies.orient.core.sql.executor;

/**
 *
 */
public class ExecutionPlanPrintUtils {

  public static void printExecutionPlan(OResultSet result) {
    printExecutionPlan(null, result);
  }

  public static void printExecutionPlan(String query, OResultSet result) {
    //    if (query != null) {
    //      System.out.println(query);
    //    }
    //    result.getExecutionPlan().ifPresent(x -> System.out.println(x.prettyPrint(0, 3)));
    //    System.out.println();
  }
}
