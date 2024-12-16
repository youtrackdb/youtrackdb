package com.jetbrains.youtrack.db.internal.core.metadata.security;

import com.jetbrains.youtrack.db.api.exception.SecurityAccessException;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

public class TestReaderDropClass extends DbTestBase {

  @Test
  public void testReaderDropClass() {
    db.getMetadata().getSchema().createClass("ReaderDropClass");
    db.close();
    db = openDatabase(readerUser, readerPassword);
    try {
      db.getMetadata().getSchema().dropClass("ReaderDropClass");
      Assert.fail("reader should not be able to drop a class");
    } catch (SecurityAccessException ex) {
    }
    db.close();
    db = openDatabase();
    Assert.assertTrue(db.getMetadata().getSchema().existsClass("ReaderDropClass"));
  }
}
