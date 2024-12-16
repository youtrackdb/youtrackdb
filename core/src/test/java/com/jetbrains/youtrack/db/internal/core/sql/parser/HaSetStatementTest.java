package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class HaSetStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA SET DBSTATUS china = 'OFFLINE'");
  }
}
