package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class HaRemoveServerStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA REMOVE SERVER foo");
    checkRightSyntax("ha remove server foo");

    checkWrongSyntax("ha remove server");
  }
}
