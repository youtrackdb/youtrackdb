package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v1;

import com.jetbrains.youtrack.db.internal.common.io.OFileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.encryption.OEncryption;
import com.jetbrains.youtrack.db.internal.core.encryption.OEncryptionFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.OLinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.File;

public class SBTreeV1TestEncryptionTestIT extends SBTreeV1TestIT {

  @Override
  public void before() throws Exception {
    buildDirectory = System.getProperty("buildDirectory", ".");

    dbName = "localSBTreeEncryptedTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    OFileUtils.deleteRecursively(dbDirectory);

    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, YouTrackDBConfig.defaultConfig());

    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");
    databaseDocumentTx = youTrackDB.open(dbName, "admin", "admin");

    sbTree =
        new OSBTreeV1<>(
            "sbTreeEncrypted",
            ".sbt",
            ".nbt",
            (OAbstractPaginatedStorage)
                ((YTDatabaseSessionInternal) databaseDocumentTx).getStorage());
    storage =
        (OAbstractPaginatedStorage) ((YTDatabaseSessionInternal) databaseDocumentTx).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    final OEncryption encryption =
        OEncryptionFactory.INSTANCE.getEncryption("aes/gcm", "T1JJRU5UREJfSVNfQ09PTA==");
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(
                atomicOperation,
                OIntegerSerializer.INSTANCE,
                OLinkSerializer.INSTANCE,
                null,
                1,
                false,
                encryption));
  }
}
