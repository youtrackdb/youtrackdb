package com.jetbrains.youtrack.db.auto;

import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class RecordCreateTest extends BaseDBTest {

  @Parameters(value = "remote")
  public RecordCreateTest(boolean remote) {
    super(remote);
  }

  @Test
  public void testNewRecordNoTx() {
    var element = db.newEntity();
    Assert.assertFalse(element.exists());
  }

  @Test
  public void testNewRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      Assert.assertFalse(element.exists());
    } finally {
      db.rollback();
    }
  }

  @Test
  public void testSavedRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      element.save();
      Assert.assertTrue(element.exists());
    } finally {
      db.rollback();
    }
  }

  @Test
  public void testDeletedRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      element.save();
      element.delete();
      Assert.assertFalse(element.exists());
    } finally {
      db.rollback();
    }
  }

  @Test
  public void testSaveDeletedRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      element.save();
      Assert.assertTrue(element.exists());
      element.delete();
      Assert.assertFalse(element.exists());
    } finally {
      db.rollback();
    }
  }

  @Test
  public void testLoadedRecordNoTx() {
    var element = db.newEntity();
    db.begin();
    element.save();
    db.commit();

    var loadedElement = db.load(element.getIdentity());
    Assert.assertTrue(loadedElement.exists());
  }

  @Test
  public void testLoadedRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      element.save();
      var loadedElement = db.load(element.getIdentity());
      Assert.assertTrue(loadedElement.exists());
    } finally {
      db.rollback();
    }
  }

  @Test
  public void testLoadedDeletedRecordTx() {
    db.begin();
    try {
      var element = db.newEntity();
      element.save();
      var loadedElement = db.load(element.getIdentity());
      Assert.assertTrue(loadedElement.exists());
      element.delete();
      Assert.assertFalse(loadedElement.exists());
    } finally {
      db.rollback();
    }
  }
}
