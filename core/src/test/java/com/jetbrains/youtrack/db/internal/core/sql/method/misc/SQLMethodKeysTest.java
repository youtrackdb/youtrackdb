package com.jetbrains.youtrack.db.internal.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultInternal;
import java.util.Arrays;
import java.util.LinkedHashSet;
import org.junit.Before;
import org.junit.Test;

public class SQLMethodKeysTest extends DbTestBase {

  private SQLMethodKeys function;

  @Before
  public void setup() {
    function = new SQLMethodKeys();
  }

  @Test
  public void testWithOResult() {

    var resultInternal = new ResultInternal(session);
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    var result = function.execute(null, null, null, resultInternal, null);
    assertEquals(new LinkedHashSet(Arrays.asList("name", "surname")), result);
  }
}
