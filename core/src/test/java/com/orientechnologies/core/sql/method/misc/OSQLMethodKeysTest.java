package com.orientechnologies.core.sql.method.misc;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.sql.executor.YTResultInternal;
import com.orientechnologies.core.sql.method.misc.OSQLMethodKeys;
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
