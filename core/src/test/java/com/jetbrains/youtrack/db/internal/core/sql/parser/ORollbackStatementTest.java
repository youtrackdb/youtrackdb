package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class ORollbackStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ROLLBACK");
    checkRightSyntax("rollback");

    checkWrongSyntax("ROLLBACK RETRY 10");
  }
}
