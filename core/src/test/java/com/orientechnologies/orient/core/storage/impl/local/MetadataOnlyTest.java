package com.orientechnologies.orient.core.storage.impl.local;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.db.OxygenDBInternal;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class MetadataOnlyTest {

  private OxygenDB oxygenDb;

  @Before
  public void before() {
    oxygenDb =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CLASS_MINIMUM_CLUSTERS, 1)
                .build());
    oxygenDb.execute(
        "create database testMetadataOnly plocal users (admin identified by 'admin' role admin)");
  }

  @Test
  public void test() {
    ODatabaseSession db = oxygenDb.open("testMetadataOnly", "admin", "admin");
    byte[] blob =
        new byte[]{
            1, 2, 3, 4, 5, 6,
        };
    ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) db).getStorage()).metadataOnly(blob);
    db.close();
    OxygenDBInternal.extract(oxygenDb).forceDatabaseClose("testMetadataOnly");
    db = oxygenDb.open("testMetadataOnly", "admin", "admin");
    Optional<byte[]> loaded =
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) db).getStorage())
            .getLastMetadata();
    assertTrue(loaded.isPresent());
    assertArrayEquals(loaded.get(), blob);
    db.close();
  }

  @After
  public void after() {

    oxygenDb.drop("testMetadataOnly");
    oxygenDb.close();
  }
}
