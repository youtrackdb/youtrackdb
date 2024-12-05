package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class ODropUserStatementTest extends OParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("DROP USER test");

    checkWrongSyntax("DROP USER test IDENTIFIED BY 'foo'");
    checkWrongSyntax("DROP USER");
  }
}
