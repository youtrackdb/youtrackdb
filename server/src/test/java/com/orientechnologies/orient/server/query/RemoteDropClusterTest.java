package com.orientechnologies.orient.server.query;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import org.junit.Test;

/**
 *
 */
public class RemoteDropClusterTest extends BaseServerMemoryDatabase {

  public void beforeTest() {
    GlobalConfiguration.CLASS_MINIMUM_CLUSTERS.setValue(1);
    super.beforeTest();
  }

  @Test
  public void simpleDropCluster() {
    int cl = db.addCluster("one");
    db.dropCluster(cl);
  }

  @Test
  public void simpleDropClusterTruncate() {
    int cl = db.addCluster("one");
    db.dropCluster(cl);
  }

  @Test
  public void simpleDropClusterName() {
    db.addCluster("one");
    db.dropCluster("one");
  }

  @Test
  public void simpleDropClusterNameTruncate() {
    db.addCluster("one");
    db.dropCluster("one");
  }
}
