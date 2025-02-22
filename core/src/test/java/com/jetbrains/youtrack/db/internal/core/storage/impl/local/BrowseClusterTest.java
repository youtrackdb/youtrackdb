package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class BrowseClusterTest {

  private DatabaseSession db;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    youTrackDb =
        new YouTrackDBImpl(
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addGlobalConfigurationParameter(GlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .addGlobalConfigurationParameter(GlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDb.execute(
        "create database "
            + "test"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + CreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    db = youTrackDb.open("test", "admin", CreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.createVertexClass("One");
  }

  @Test
  public void testBrowse() {
    int numberOfEntries = 4962;
    for (int i = 0; i < numberOfEntries; i++) {
      db.begin();
      Vertex v = db.newVertex("One");
      v.setProperty("a", i);
      db.save(v);
      db.commit();
    }
    int cluster = db.getClass("One").getClusterIds()[0];
    Iterator<ClusterBrowsePage> browser =
        ((AbstractPaginatedStorage) ((DatabaseSessionInternal) db).getStorage())
            .browseCluster(cluster);
    int count = 0;

    while (browser.hasNext()) {
      ClusterBrowsePage page = browser.next();
      for (ClusterBrowseEntry entry : page) {
        count++;
        assertNotNull(entry.getBuffer());
        assertNotNull(entry.getClusterPosition());
      }
    }
    assertEquals(numberOfEntries, count);
  }

  @After
  public void after() {
    db.close();
    youTrackDb.close();
  }
}
