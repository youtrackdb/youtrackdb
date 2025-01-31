package com.jetbrains.youtrack.db.internal.server.query;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.server.BaseServerMemoryDatabase;
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
    var cl = db.addCluster("one");
    db.dropCluster(cl);
  }

  @Test
  public void simpleDropClusterTruncate() {
    var cl = db.addCluster("one");
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
