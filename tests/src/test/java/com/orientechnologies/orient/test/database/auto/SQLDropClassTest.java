package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import org.testng.Assert;
import org.testng.annotations.Test;

/**
 *
 */
public class SQLDropClassTest {

  @Test
  public void testSimpleDrop() {
    YTDatabaseSessionInternal db =
        new YTDatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName());
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
    YTDatabaseSessionInternal db =
        new YTDatabaseDocumentTx("memory:" + SQLDropClassTest.class.getName() + "_ifNotExists");
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
