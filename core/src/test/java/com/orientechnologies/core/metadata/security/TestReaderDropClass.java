package com.orientechnologies.core.metadata.security;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.core.exception.YTSecurityAccessException;
import org.junit.Assert;
import org.junit.Test;

public class TestReaderDropClass {

  @Test
  public void testReaderDropClass() {
    YTDatabaseSessionInternal db =
        new YTDatabaseDocumentTx("memory:" + TestReaderDropClass.class.getSimpleName());
    db.create();
    try {
      db.getMetadata().getSchema().createClass("Test");
      db.close();
      db.open("reader", "reader");
      try {
        db.getMetadata().getSchema().dropClass("Test");
        Assert.fail("reader should not be able to drop a class");
      } catch (YTSecurityAccessException ex) {
      }
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("Test"));

    } finally {
      db.close();
      db.open("admin", "admin");
      db.drop();
    }
  }
}
