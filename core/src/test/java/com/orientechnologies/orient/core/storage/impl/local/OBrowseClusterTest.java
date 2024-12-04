package com.orientechnologies.orient.core.storage.impl.local;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Iterator;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OBrowseClusterTest {

  private ODatabaseSession db;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    youTrackDb.execute(
        "create database "
            + "test"
            + " "
            + "memory"
            + " users ( admin identified by '"
            + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
            + "' role admin)");
    db = youTrackDb.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    db.createVertexClass("One");
  }

  @Test
  public void testBrowse() {
    int numberOfEntries = 4962;
    for (int i = 0; i < numberOfEntries; i++) {
      db.begin();
      OVertex v = db.newVertex("One");
      v.setProperty("a", i);
      db.save(v);
      db.commit();
    }
    int cluster = db.getClass("One").getDefaultClusterId();
    Iterator<OClusterBrowsePage> browser =
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) db).getStorage())
            .browseCluster(cluster);
    int count = 0;

    while (browser.hasNext()) {
      OClusterBrowsePage page = browser.next();
      for (OClusterBrowseEntry entry : page) {
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
