package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v3;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.BinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.MurmurHash3HashFunction;
import java.io.File;
import org.junit.After;
import org.junit.Before;

/**
 * @since 19.02.13
 */
public class LocalHashTableV3TestIT extends LocalHashTableV3Base {

  private YouTrackDB youTrackDB;

  private static final String DB_NAME = "localHashTableTest";

  @Before
  public void before() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    FileUtils.deleteRecursively(dbDirectory);
    youTrackDB = new YouTrackDBImpl("plocal:" + buildDirectory,
        YouTrackDBConfig.defaultConfig());

    youTrackDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");
    final DatabaseSession databaseDocumentTx = youTrackDB.open(DB_NAME, "admin", "admin");

    MurmurHash3HashFunction<Integer> murmurHash3HashFunction =
        new MurmurHash3HashFunction<Integer>(IntegerSerializer.INSTANCE);

    localHashTable =
        new LocalHashTableV3<>(
            "localHashTableTest",
            ".imc",
            ".tsc",
            ".obf",
            ".nbh",
            (AbstractPaginatedStorage)
                ((DatabaseSessionInternal) databaseDocumentTx).getStorage());

    atomicOperationsManager =
        ((AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage())
            .getAtomicOperationsManager();
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localHashTable.create(
                atomicOperation,
                IntegerSerializer.INSTANCE,
                BinarySerializerFactory.getInstance().getObjectSerializer(PropertyType.STRING),
                null,
                null,
                murmurHash3HashFunction,
                true));
  }

  @After
  public void after() {
    youTrackDB.drop(DB_NAME);
    youTrackDB.close();
  }
}
