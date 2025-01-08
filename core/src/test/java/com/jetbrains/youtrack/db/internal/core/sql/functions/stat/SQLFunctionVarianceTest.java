package com.jetbrains.youtrack.db.internal.core.sql.functions.stat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Before;
import org.junit.Test;

public class SQLFunctionVarianceTest {

  private SQLFunctionVariance variance;

  @Before
  public void setup() {
    variance =
        new SQLFunctionVariance() {
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
}
