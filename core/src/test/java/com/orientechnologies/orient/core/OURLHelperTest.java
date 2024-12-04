package com.orientechnologies.orient.core;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.orient.core.exception.YTConfigurationException;
import com.orientechnologies.orient.core.util.OURLConnection;
import com.orientechnologies.orient.core.util.OURLHelper;
import java.io.File;
import org.junit.Test;

/**
 *
 */
public class OURLHelperTest {

  @Test
  public void testSimpleUrl() {
    OURLConnection parsed = OURLHelper.parse("plocal:/path/test/to");
    assertEquals("plocal", parsed.getType());
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
    assertEquals("to", parsed.getDbName());

    parsed = OURLHelper.parse("memory:some");
    assertEquals("memory", parsed.getType());
    // assertEquals(parsed.getPath(), "");
    assertEquals("some", parsed.getDbName());

    parsed = OURLHelper.parse("remote:localhost/to");
    assertEquals("remote", parsed.getType());
    assertEquals("localhost", parsed.getPath());
    assertEquals("to", parsed.getDbName());
  }

  @Test
  public void testSimpleNewUrl() {
    OURLConnection parsed = OURLHelper.parseNew("plocal:/path/test/to");
    assertEquals("embedded", parsed.getType());
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
    assertEquals("to", parsed.getDbName());

    parsed = OURLHelper.parseNew("memory:some");
    assertEquals("embedded", parsed.getType());
    assertEquals("", parsed.getPath());
    assertEquals("some", parsed.getDbName());

    parsed = OURLHelper.parseNew("embedded:/path/test/to");
    assertEquals("embedded", parsed.getType());
    assertEquals(parsed.getPath(), new File("/path/test").getAbsolutePath());
    assertEquals("to", parsed.getDbName());

    parsed = OURLHelper.parseNew("remote:localhost/to");
    assertEquals("remote", parsed.getType());
    assertEquals("localhost", parsed.getPath());
    assertEquals("to", parsed.getDbName());
  }

  @Test(expected = YTConfigurationException.class)
  public void testWrongPrefix() {
    OURLHelper.parseNew("embd:/path/test/to");
  }

  @Test(expected = YTConfigurationException.class)
  public void testNoPrefix() {
    OURLHelper.parseNew("/embd/path/test/to");
  }

  @Test()
  public void testRemoteNoDatabase() {
    OURLConnection parsed = OURLHelper.parseNew("remote:localhost");
    assertEquals("remote", parsed.getType());
    assertEquals("localhost", parsed.getPath());
    assertEquals("", parsed.getDbName());

    parsed = OURLHelper.parseNew("remote:localhost:2424");
    assertEquals("remote", parsed.getType());
    assertEquals("localhost:2424", parsed.getPath());
    assertEquals("", parsed.getDbName());

    parsed = OURLHelper.parseNew("remote:localhost:2424/db1");
    assertEquals("remote", parsed.getType());
    assertEquals("localhost:2424", parsed.getPath());
    assertEquals("db1", parsed.getDbName());
  }
}
