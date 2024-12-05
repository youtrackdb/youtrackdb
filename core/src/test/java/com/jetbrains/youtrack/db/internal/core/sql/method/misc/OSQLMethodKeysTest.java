package com.jetbrains.youtrack.db.internal.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultInternal;
import java.util.Arrays;
import java.util.LinkedHashSet;
import org.junit.Before;
import org.junit.Test;

public class OSQLMethodKeysTest extends DBTestBase {

  private OSQLMethodKeys function;

  @Before
  public void setup() {
    function = new OSQLMethodKeys();
  }

  @Test
  public void testWithOResult() {

    YTResultInternal resultInternal = new YTResultInternal(db);
    resultInternal.setProperty("name", "Foo");
    resultInternal.setProperty("surname", "Bar");

    Object result = function.execute(null, null, null, resultInternal, null);
    assertEquals(new LinkedHashSet(Arrays.asList("name", "surname")), result);
  }
}
