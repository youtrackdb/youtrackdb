package com.orientechnologies.orient.core.tx;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionMetadataTest {

  private OxygenDB oxygenDB;
  private ODatabaseSessionInternal db;
  private static final String DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {
    oxygenDB =
        OCreateDatabaseUtil.createDatabase(
            DB_NAME, "embedded:./target/", OCreateDatabaseUtil.TYPE_PLOCAL);
    db =
        (ODatabaseSessionInternal)
            oxygenDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
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
    oxygenDB.close();

    oxygenDB =
        new OxygenDB(
            "embedded:./target/",
            OxygenDBConfig.builder()
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    db =
        (ODatabaseSessionInternal)
            oxygenDB.open(DB_NAME, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    Optional<byte[]> fromStorage = ((OAbstractPaginatedStorage) db.getStorage()).getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @After
  public void after() {
    db.close();
    oxygenDB.drop(DB_NAME);
    if (oxygenDB.exists(DB_NAME + "_re")) {
      oxygenDB.drop(DB_NAME + "_re");
    }
    oxygenDB.close();
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
