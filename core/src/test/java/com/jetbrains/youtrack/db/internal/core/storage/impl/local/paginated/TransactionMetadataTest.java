package com.jetbrains.youtrack.db.internal.core.storage.impl.local.paginated;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseType;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionId;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionInternal;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionSequenceStatus;
import com.jetbrains.youtrack.db.internal.core.tx.OTxMetadataHolder;
import java.io.File;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TransactionMetadataTest {

  private YouTrackDB youTrackDB;
  private YTDatabaseSessionInternal db;
  private static final String DB_NAME = TransactionMetadataTest.class.getSimpleName();

  @Before
  public void before() {

    youTrackDB = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database `" + DB_NAME + "` plocal users(admin identified by 'admin' role admin)");
    db = (YTDatabaseSessionInternal) youTrackDB.open(DB_NAME, "admin", "admin");
  }

  @Test
  public void testBackupRestore() {
    db.begin();
    byte[] metadata = new byte[]{1, 2, 4};
    ((OTransactionInternal) db.getTransaction())
        .setMetadataHolder(new TestMetadataHolder(metadata));
    Vertex v = db.newVertex("V");
    v.setProperty("name", "Foo");
    db.save(v);
    db.commit();
    db.incrementalBackup("target/backup_metadata");
    db.close();
    YouTrackDBInternal.extract(youTrackDB)
        .restore(
            DB_NAME + "_re",
            null,
            null,
            ODatabaseType.PLOCAL,
            "target/backup_metadata",
            YouTrackDBConfig.defaultConfig());
    YTDatabaseSession db1 = youTrackDB.open(DB_NAME + "_re", "admin", "admin");
    Optional<byte[]> fromStorage =
        ((AbstractPaginatedStorage) ((YTDatabaseSessionInternal) db1).getStorage())
            .getLastMetadata();
    assertTrue(fromStorage.isPresent());
    assertArrayEquals(fromStorage.get(), metadata);
  }

  @After
  public void after() {
    FileUtils.deleteRecursively(new File("target/backup_metadata"));
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
