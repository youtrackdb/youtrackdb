package com.orientechnologies.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Test;

public class TestSelectDetectType extends DBTestBase {

  @Test
  public void testFloatDetection() {
    YTResultSet res = db.query("select ty.type() as ty from ( select 1.021484375 as ty)");
    assertEquals(res.next().getProperty("ty"), "FLOAT");
    res = db.query("select ty.type() as ty from ( select " + Float.MAX_VALUE + "0101 as ty)");
    assertEquals(res.next().getProperty("ty"), "DOUBLE");
  }
}
