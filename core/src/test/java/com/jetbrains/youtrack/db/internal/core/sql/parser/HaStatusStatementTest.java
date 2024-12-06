package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class HaStatusStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA STATUS -servers");
    checkRightSyntax("HA STATUS -db");
    checkRightSyntax("HA STATUS -latency");
    checkRightSyntax("HA STATUS -messages");
    checkRightSyntax("HA STATUS -locks");
    checkRightSyntax("HA STATUS -all");
    checkRightSyntax("HA STATUS -all -output=text");
    checkRightSyntax("HA STATUS -servers -db -latency -messages -locks ");

    checkWrongSyntax("HA STATUS servers");
  }
}
