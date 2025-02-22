package com.jetbrains.youtrack.db.internal.core.sql.functions.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Before;
import org.junit.Test;

public class SQLFunctionVarianceTest {

  private SQLFunctionVariance variance;

  @Before
  public void setup() {
    variance =
        new SQLFunctionVariance() {
          @Override
          protected boolean returnDistributedResult() {
            return false;
          }
        };
  }

  @Test
  public void testEmpty() {
    Object result = variance.getResult();
    assertNull(result);
  }

  @Test
  public void testVariance() {
    Integer[] scores = {4, 7, 15, 3};

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[]{s}, null);
    }

    Object result = variance.getResult();
    assertEquals(22.1875, result);
  }

  @Test
  public void testVariance1() {
    Integer[] scores = {4, 7};

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[]{s}, null);
    }

    Object result = variance.getResult();
    assertEquals(2.25, result);
  }

  @Test
  public void testVariance2() {
    Integer[] scores = {15, 3};

    for (Integer s : scores) {
      variance.execute(null, null, null, new Object[]{s}, null);
    }

    Object result = variance.getResult();
    assertEquals(36.0, result);
  }

  @Test
  public void testDistributed() {
    Map<String, Object> doc1 = new HashMap<String, Object>();
    doc1.put("n", 2L);
    doc1.put("mean", 5.5);
    doc1.put("var", 2.25);

    Map<String, Object> doc2 = new HashMap<String, Object>();
    doc2.put("n", 2L);
    doc2.put("mean", 9d);
    doc2.put("var", 36d);

    List<Object> results = new ArrayList<Object>(2);
    results.add(doc1);
    results.add(doc2);

    assertEquals(
        22.1875,
        new SQLFunctionVariance() {
          @Override
          protected boolean returnDistributedResult() {
            return true;
          }
        }.mergeDistributedResult(results));
  }
}
