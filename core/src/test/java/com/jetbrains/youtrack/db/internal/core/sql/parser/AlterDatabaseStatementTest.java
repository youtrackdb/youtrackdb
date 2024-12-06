package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class AlterDatabaseStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER DATABASE CLUSTERSELECTION 'default'");
    checkRightSyntax("alter database CLUSTERSELECTION 'default'");

    checkRightSyntax("alter database custom strictSql=false");

    checkWrongSyntax("alter database ");
    checkWrongSyntax("alter database bar baz zz");
  }
}
