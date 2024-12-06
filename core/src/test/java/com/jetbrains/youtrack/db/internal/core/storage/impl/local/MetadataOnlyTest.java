package com.jetbrains.youtrack.db.internal.core.storage.impl.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
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
            DbTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(GlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build());
    youTrackDb.execute(
        "create database testMetadataOnly plocal users (admin identified by 'admin' role admin)");
  }

  @Test
  public void test() {
    DatabaseSession db = youTrackDb.open("testMetadataOnly", "admin", "admin");
    byte[] blob =
        new byte[]{
            1, 2, 3, 4, 5, 6,
        };
    ((AbstractPaginatedStorage) ((DatabaseSessionInternal) db).getStorage()).metadataOnly(blob);
    db.close();
    YouTrackDBInternal.extract(youTrackDb).forceDatabaseClose("testMetadataOnly");
    db = youTrackDb.open("testMetadataOnly", "admin", "admin");
    Optional<byte[]> loaded =
        ((AbstractPaginatedStorage) ((DatabaseSessionInternal) db).getStorage())
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
