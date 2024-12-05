package com.orientechnologies.core.record.impl;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.record.YTEntity;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class YTEntityTest extends DBTestBase {

  @Test
  public void testGetSetProperty() {
    YTEntity elem = db.newEntity();
    elem.setProperty("foo", "foo1");
    elem.setProperty("foo.bar", "foobar");
    elem.setProperty("  ", "spaces");

    var names = elem.getPropertyNames();
    Assert.assertTrue(names.contains("foo"));
    Assert.assertTrue(names.contains("foo.bar"));
    Assert.assertTrue(names.contains("  "));
  }

  @Test
  public void testLoadAndSave() {
    db.createClassIfNotExist("TestLoadAndSave");
    db.begin();
    YTEntity elem = db.newEntity("TestLoadAndSave");
    elem.setProperty("name", "foo");
    db.save(elem);
    db.commit();

    YTResultSet result = db.query("select from TestLoadAndSave where name = 'foo'");
    Assert.assertTrue(result.hasNext());
    Assert.assertEquals("foo", result.next().getProperty("name"));
    result.close();
  }
}
