package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OIdentifierTest {

  @Test
  public void testBackTickQuoted() {
    SQLIdentifier identifier = new SQLIdentifier("foo`bar");

    Assert.assertEquals(identifier.getStringValue(), "foo`bar");
    Assert.assertEquals(identifier.getValue(), "foo\\`bar");
  }
}
