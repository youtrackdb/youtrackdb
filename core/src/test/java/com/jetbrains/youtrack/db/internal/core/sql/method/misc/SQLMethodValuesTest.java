package com.jetbrains.youtrack.db.internal.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.Arrays;
import org.junit.Before;
import org.junit.Test;

public class SQLMethodValuesTest extends DbTestBase {

  private SQLMethodValues function;

  @Before
  public void setup() {
    function = new SQLMethodValues();
  }

  @Test
  public void testWithOResult() {

    var resultInternal = new ResultInternal(db);
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    var result = function.execute(null, null, null, resultInternal, null);
    assertEquals(Arrays.asList("Foo", "Bar"), result);
  }
}
