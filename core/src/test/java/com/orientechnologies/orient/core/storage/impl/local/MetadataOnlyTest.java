package com.orientechnologies.orient.core.storage.impl.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.YouTrackDBInternal;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataOnlyTest {

  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(YTGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build());
    youTrackDb.execute(
        "create database testMetadataOnly plocal users (admin identified by 'admin' role admin)");
  }

  @Test
  public void test() {
    YTDatabaseSession db = youTrackDb.open("testMetadataOnly", "admin", "admin");
    byte[] blob =
        new byte[]{
            1, 2, 3, 4, 5, 6,
        };
    ((OAbstractPaginatedStorage) ((YTDatabaseSessionInternal) db).getStorage()).metadataOnly(blob);
    db.close();
    YouTrackDBInternal.extract(youTrackDb).forceDatabaseClose("testMetadataOnly");
    db = youTrackDb.open("testMetadataOnly", "admin", "admin");
    Optional<byte[]> loaded =
        ((OAbstractPaginatedStorage) ((YTDatabaseSessionInternal) db).getStorage())
            .getLastMetadata();
    assertTrue(loaded.isPresent());
    assertArrayEquals(loaded.get(), blob);
    db.close();
  }

  @After
  public void after() {

    youTrackDb.drop("testMetadataOnly");
    youTrackDb.close();
  }
}
