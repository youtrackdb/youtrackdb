package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;

import java.io.UnsupportedEncodingException;
import org.junit.Test;

public class StringCacheTest {

  @Test
  public void testSingleAdd() throws UnsupportedEncodingException {
    byte[] bytes = "abcde".getBytes();
    StringCache cache = new StringCache(500);
    String value = cache.getString(bytes, 0, bytes.length);
    assertEquals(value, "abcde");
    assertEquals(cache.size(), 1);
  }

  @Test
  public void testDobuleHit() throws UnsupportedEncodingException {
    byte[] bytes = "abcde".getBytes();
    StringCache cache = new StringCache(500);
    String value = cache.getString(bytes, 0, bytes.length);
    byte[] other = new byte[50];
    System.arraycopy(bytes, 0, other, 10, bytes.length);
    String value1 = cache.getString(other, 10, bytes.length);
    assertEquals(value1, "abcde");
    assertSame(value, value1);
    assertEquals(cache.size(), 1);
  }
}
