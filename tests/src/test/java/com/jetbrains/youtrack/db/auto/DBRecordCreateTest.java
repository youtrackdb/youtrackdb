package com.jetbrains.youtrack.db.auto;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class DBRecordCreateTest extends BaseDBTest {

  @Parameters(value = "remote")
  public DBRecordCreateTest(boolean remote) {
    super(remote);
  }

  @Test
  public void testNewRecordNoTx() {
    var element = database.newEntity();
    Assert.assertFalse(element.exists());
  }

  @Test
  public void testNewRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      Assert.assertFalse(element.exists());
    } finally {
      database.rollback();
    }
  }

  @Test
  public void testSavedRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      element.save();
      Assert.assertTrue(element.exists());
    } finally {
      database.rollback();
    }
  }

  @Test
  public void testDeletedRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      element.save();
      element.delete();
      Assert.assertFalse(element.exists());
    } finally {
      database.rollback();
    }
  }

  @Test
  public void testSaveDeletedRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      element.save();
      Assert.assertTrue(element.exists());
      element.delete();
      Assert.assertFalse(element.exists());
    } finally {
      database.rollback();
    }
  }

  @Test
  public void testLoadedRecordNoTx() {
    var element = database.newEntity();
    database.begin();
    element.save();
    database.commit();

    var loadedElement = database.load(element.getIdentity());
    Assert.assertTrue(loadedElement.exists());
  }

  @Test
  public void testLoadedRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      element.save();
      var loadedElement = database.load(element.getIdentity());
      Assert.assertTrue(loadedElement.exists());
    } finally {
      database.rollback();
    }
  }

  @Test
  public void testLoadedDeletedRecordTx() {
    database.begin();
    try {
      var element = database.newEntity();
      element.save();
      var loadedElement = database.load(element.getIdentity());
      Assert.assertTrue(loadedElement.exists());
      element.delete();
      Assert.assertFalse(loadedElement.exists());
    } finally {
      database.rollback();
    }
  }
}
