package com.jetbrains.youtrack.db.internal.core.db;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

public class DatabaseCreateDropClusterTest extends DbTestBase {

  @Test
  public void createDropCluster() {
    session.addCluster("test");
    Assert.assertNotEquals(-1, session.getClusterIdByName("test"));
    session.dropCluster("test");
    Assert.assertEquals(-1, session.getClusterIdByName("test"));
  }

  @Test
  public void createDropClusterOnClass() {
    var test = session.getMetadata().getSchema().createClass("test", 1, null);
    test.addCluster(session, "aTest");
    Assert.assertNotEquals(-1, session.getClusterIdByName("aTest"));
    Assert.assertEquals(2, test.getClusterIds(session).length);
    session.dropCluster("aTest");
    Assert.assertEquals(-1, session.getClusterIdByName("aTest"));
    test = session.getMetadata().getSchema().getClass("test");
    Assert.assertEquals(1, test.getClusterIds(session).length);
  }
}
