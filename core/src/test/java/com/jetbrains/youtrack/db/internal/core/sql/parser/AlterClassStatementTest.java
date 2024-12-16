package com.jetbrains.youtrack.db.internal.core.sql.parser;

import org.junit.Test;

public class AlterClassStatementTest extends ParserTestAbstract {

  @Test
  public void testPlain() {
    checkRightSyntax("ALTER CLASS Foo NAME Bar");
    checkRightSyntax("alter class Foo name Bar");
    checkRightSyntax("ALTER CLASS Foo NAME Bar UNSAFE");
    checkRightSyntax("alter class Foo name Bar unsafe");

    checkRightSyntax("ALTER CLASS `Foo bar` NAME `Bar bar`");

    checkRightSyntax("ALTER CLASS Foo SHORTNAME Bar");
    checkRightSyntax("ALTER CLASS Foo shortname Bar");

    checkRightSyntax("ALTER CLASS Foo ADD_CLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo add_cluster bar");

    checkRightSyntax("ALTER CLASS Foo REMOVE_CLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo remove_cluster bar");

    checkRightSyntax("ALTER CLASS Foo DESCRIPTION bar");
    checkRightSyntax("ALTER CLASS Foo description bar");

    checkRightSyntax("ALTER CLASS Foo CLUSTER_SELECTION default");

    checkRightSyntax("ALTER CLASS Foo CLUSTER_SELECTION round-robin");
    checkRightSyntax("ALTER CLASS Foo cluster_selection round-robin");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASS Bar");
    checkRightSyntax("ALTER CLASS Foo superclass Bar");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASS +Bar");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASS -Bar");
    checkRightSyntax("ALTER CLASS Foo superclass null");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar");
    checkRightSyntax("ALTER CLASS Foo superclasses Bar");

    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar, Bazz, braz");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES Bar,Bazz,braz");
    checkRightSyntax("ALTER CLASS Foo SUPERCLASSES null");

    checkRightSyntax("ALTER CLASS Foo STRICT_MODE true");
    checkRightSyntax("ALTER CLASS Foo strict_mode true");
    checkRightSyntax("ALTER CLASS Foo STRICT_MODE false");

    checkRightSyntax("ALTER CLASS Foo ADD_CLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo add_cluster bar");

    checkRightSyntax("ALTER CLASS Foo REMOVE_CLUSTER bar");
    checkRightSyntax("ALTER CLASS Foo remove_cluster bar");

    checkRightSyntax("ALTER CLASS Foo CUSTOM bar=baz");
    checkRightSyntax("ALTER CLASS Foo custom bar=baz");
    checkRightSyntax("ALTER CLASS Foo CUSTOM bar = baz");

    checkRightSyntax("ALTER CLASS Person CUSTOM `onCreate.identityType`=role");

    checkWrongSyntax("ALTER CLASS Foo NAME Bar baz");

    checkWrongSyntax("ALTER CLASS Foo SUPERCLASS *Bar");
  }
}
