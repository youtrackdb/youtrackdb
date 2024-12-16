package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class SQLDropClassTest {

  @Test
  public void testSimpleDrop() {
    DatabaseSessionInternal db =
        new DatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName());
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
      db.command("create class testSimpleDrop").close();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
      db.command("Drop class testSimpleDrop").close();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testSimpleDrop"));
    } finally {
      db.drop();
    }
  }

  @Test
  public void testIfExists() {
    DatabaseSessionInternal db =
        new DatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName() + "_ifNotExists");
    db.create();
    try {
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("create class testIfExists if not exists").close();
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("drop class testIfExists if exists").close();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));
      db.command("drop class testIfExists if exists").close();
      Assert.assertFalse(db.getMetadata().getSchema().existsClass("testIfExists"));

    } finally {
      db.drop();
    }
  }
}
