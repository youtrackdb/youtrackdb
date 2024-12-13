package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.IntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfigImpl;
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
public class LocalHashTableV2TestIT extends LocalHashTableV2Base {

  private YouTrackDB youTrackDB;

  private static final String DB_NAME = "localHashTableTest";

  @Before
  public void before() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    FileUtils.deleteRecursively(dbDirectory);
    final YouTrackDBConfigImpl config = (YouTrackDBConfigImpl) YouTrackDBConfig.builder().build();
    youTrackDB = new YouTrackDBImpl("plocal:" + buildDirectory, config);

    youTrackDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");
    final DatabaseSession databaseDocumentTx = youTrackDB.open(DB_NAME, "admin", "admin", config);
    storage =
        (AbstractPaginatedStorage) ((DatabaseSessionInternal) databaseDocumentTx).getStorage();
    MurmurHash3HashFunction<Integer> murmurHash3HashFunction =
        new MurmurHash3HashFunction<Integer>(IntegerSerializer.INSTANCE);

    localHashTable =
        new LocalHashTableV2<>("localHashTableTest", ".imc", ".tsc", ".obf", ".nbh", storage);

    storage
        .getAtomicOperationsManager()
        .executeInsideAtomicOperation(
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
