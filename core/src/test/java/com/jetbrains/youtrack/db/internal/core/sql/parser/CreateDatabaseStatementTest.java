package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class CreateDatabaseStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntaxServer("CREATE DATABASE foo plocal");
    checkRightSyntaxServer("CREATE DATABASE ? plocal");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal {\"config\":{\"security.createDefaultUsers\": true}}");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)");
    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin, reader identified"
            + " by ? role [reader, writer])");

    checkRightSyntaxServer(
        "CREATE DATABASE foo plocal users (foo identified by 'pippo' role admin)"
            + " {\"config\":{\"security.createDefaultUsers\": true}}");

    checkWrongSyntax("CREATE DATABASE foo");
    checkWrongSyntax("CREATE DATABASE");
  }
}
