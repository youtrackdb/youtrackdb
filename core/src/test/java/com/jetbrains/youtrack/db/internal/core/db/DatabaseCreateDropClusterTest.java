package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseCreateDropClusterTest extends DbTestBase {

  @Test
  public void createDropCluster() {
    db.addCluster("test");
    Assert.assertNotEquals(db.getClusterIdByName("test"), -1);
    db.dropCluster("test");
    Assert.assertEquals(db.getClusterIdByName("test"), -1);
  }

  @Test
  public void createDropClusterOnClass() {
    SchemaClass test = db.getMetadata().getSchema().createClass("test", 1, null);
    test.addCluster(db, "aTest");
    Assert.assertNotEquals(db.getClusterIdByName("aTest"), -1);
    Assert.assertEquals(test.getClusterIds().length, 2);
    db.dropCluster("aTest");
    Assert.assertEquals(db.getClusterIdByName("aTest"), -1);
    test = db.getMetadata().getSchema().getClass("test");
    Assert.assertEquals(test.getClusterIds().length, 1);
  }
}
