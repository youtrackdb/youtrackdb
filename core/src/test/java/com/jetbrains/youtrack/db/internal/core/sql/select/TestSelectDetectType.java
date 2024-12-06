package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.junit.Test;

public class TestSelectDetectType extends DbTestBase {

  @Test
  public void testFloatDetection() {
    ResultSet res = db.query("select ty.type() as ty from ( select 1.021484375 as ty)");
    assertEquals(res.next().getProperty("ty"), "FLOAT");
    res = db.query("select ty.type() as ty from ( select " + Float.MAX_VALUE + "0101 as ty)");
    assertEquals(res.next().getProperty("ty"), "DOUBLE");
  }
}
