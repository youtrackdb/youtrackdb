package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class HaSyncClusterStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("HA SYNC CLUSTER foo");
    checkRightSyntax("ha sync cluster foo");
    checkRightSyntax("HA SYNC CLUSTER foo -full_replace");
    checkRightSyntax("HA SYNC CLUSTER foo -merge");

    checkWrongSyntax("HA SYNC CLUSTER foo -foo");
    checkWrongSyntax("HA SYNC CLUSTER");
  }
}
