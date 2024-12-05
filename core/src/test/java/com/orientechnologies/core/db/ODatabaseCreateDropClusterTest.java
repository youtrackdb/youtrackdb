package com.orientechnologies.core.db;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTClass;
import org.junit.Assert;
import org.junit.Test;

public class ODatabaseCreateDropClusterTest extends DBTestBase {

  @Test
  public void createDropCluster() {
    db.addCluster("test");
    Assert.assertNotEquals(db.getClusterIdByName("test"), -1);
    db.dropCluster("test");
    Assert.assertEquals(db.getClusterIdByName("test"), -1);
  }

  @Test
  public void createDropClusterOnClass() {
    YTClass test = db.getMetadata().getSchema().createClass("test", 1, null);
    test.addCluster(db, "aTest");
    Assert.assertNotEquals(db.getClusterIdByName("aTest"), -1);
    Assert.assertEquals(test.getClusterIds().length, 2);
    db.dropCluster("aTest");
    Assert.assertEquals(db.getClusterIdByName("aTest"), -1);
    test = db.getMetadata().getSchema().getClass("test");
    Assert.assertEquals(test.getClusterIds().length, 1);
  }
}
