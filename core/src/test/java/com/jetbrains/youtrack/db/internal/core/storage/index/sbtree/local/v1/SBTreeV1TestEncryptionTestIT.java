package com.jetbrains.youtrack.db.internal.core.storage.index.sbtree.local.v1;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.encryption.EncryptionFactory;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.impl.LinkSerializer;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import java.io.File;

public class SBTreeV1TestEncryptionTestIT extends SBTreeV1TestIT {

  @Override
  public void before() throws Exception {
    buildDirectory = System.getProperty("buildDirectory", ".");

    dbName = "localSBTreeEncryptedTest";
    final File dbDirectory = new File(buildDirectory, dbName);
    FileUtils.deleteRecursively(dbDirectory);

    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, YouTrackDBConfig.defaultConfig());

    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");
    databaseDocumentTx = youTrackDB.open(dbName, "admin", "admin");

    sbTree =
        new SBTreeV1<>(
            "sbTreeEncrypted",
            ".sbt",
            ".nbt",
            (AbstractPaginatedStorage)
                ((DatabaseSessionInternal) databaseDocumentTx).getStorage());
    storage =
        (AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();
    atomicOperationsManager = storage.getAtomicOperationsManager();
    final Encryption encryption =
        EncryptionFactory.INSTANCE.getEncryption("aes/gcm", "T1JJRU5UREJfSVNfQ09PTA==");
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            sbTree.create(
                atomicOperation,
                IntegerSerializer.INSTANCE,
                LinkSerializer.INSTANCE,
                null,
                1,
                false,
                encryption));
  }
}
