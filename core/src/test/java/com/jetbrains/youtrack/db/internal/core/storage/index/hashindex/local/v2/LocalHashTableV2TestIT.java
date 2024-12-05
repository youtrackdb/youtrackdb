package com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.v2;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.serialization.types.OIntegerSerializer;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.jetbrains.youtrack.db.internal.core.storage.impl.local.AbstractPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
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
    final YouTrackDBConfig config = YouTrackDBConfig.builder().build();
    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, config);

    youTrackDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");
    final YTDatabaseSession databaseDocumentTx = youTrackDB.open(DB_NAME, "admin", "admin", config);
    storage =
        (AbstractPaginatedStorage) ((YTDatabaseSessionInternal) databaseDocumentTx).getStorage();
    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction =
        new OMurmurHash3HashFunction<Integer>(OIntegerSerializer.INSTANCE);

    localHashTable =
        new LocalHashTableV2<>("localHashTableTest", ".imc", ".tsc", ".obf", ".nbh", storage);

    storage
        .getAtomicOperationsManager()
        .executeInsideAtomicOperation(
            null,
            atomicOperation ->
                localHashTable.create(
                    atomicOperation,
                    OIntegerSerializer.INSTANCE,
                    OBinarySerializerFactory.getInstance().getObjectSerializer(YTType.STRING),
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
