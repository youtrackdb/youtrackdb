package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionMetadataTest {

  private YouTrackDB youTrackDB;
  private ODatabaseSessionInternal db;
  private static final String DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase(
            DB_NAME, DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_PLOCAL);
    db =
        (ODatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
  }

  @Test
  public void test() {
    db.begin();
    byte[] metadata = new byte[]{1, 2, 4};
    ((OTransactionInternal) db.getTransaction())
        .setMetadataHolder(new TestMetadataHolder(metadata));
    OVertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    db.close();
    youTrackDB.close();

    youTrackDB =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        (ODatabaseSessionInternal)
            youTrackDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    Optional<byte[]> fromStorage = ((OAbstractPaginatedStorage) db.getStorage()).getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @After
  public void after() {
    db.close();
    youTrackDB.drop(DB_NAME);
    if (youTrackDB.exists(DB_NAME + "_re")) {
      youTrackDB.drop(DB_NAME + "_re");
    }
    youTrackDB.close();
  }

  private static class TestMetadataHolder implements OTxMetadataHolder {

    private final byte[] metadata;

    public TestMetadataHolder(byte[] metadata) {
      this.metadata = metadata;
    }

    @Override
    public byte[] metadata() {
      return metadata;
    }

    @Override
    public void notifyMetadataRead() {
    }

    @Override
    public OTransactionId getId() {
      return null;
    }

    @Override
    public OTransactionSequenceStatus getStatus() {
      return null;
    }
  }
}
