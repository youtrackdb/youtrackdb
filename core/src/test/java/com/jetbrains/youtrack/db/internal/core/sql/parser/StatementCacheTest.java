package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

public class StatementCacheTest {

  @Test
  public void testInIsNotAReservedWord() {
    var cache = new StatementCache(2);
    cache.get("select from foo");
    cache.get("select from bar");
    cache.get("select from baz");

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from baz"));
    Assert.assertFalse(cache.contains("select from foo"));

    cache.get("select from bar");
    cache.get("select from foo");

    Assert.assertTrue(cache.contains("select from bar"));
    Assert.assertTrue(cache.contains("select from foo"));
    Assert.assertFalse(cache.contains("select from baz"));
  }
}
