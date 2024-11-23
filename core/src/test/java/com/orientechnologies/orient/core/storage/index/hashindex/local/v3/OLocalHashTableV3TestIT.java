package com.orientechnologies.orient.core.storage.index.hashindex.local.v3;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OMurmurHash3HashFunction;
import java.io.File;
import org.junit.After;
import org.junit.Before;

/**
 * @since 19.02.13
 */
public class OLocalHashTableV3TestIT extends OLocalHashTableV3Base {

  private OxygenDB oxygenDB;

  private static final String DB_NAME = "localHashTableTest";

  @Before
  public void before() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    OFileUtils.deleteRecursively(dbDirectory);
    oxygenDB = new OxygenDB("plocal:" + buildDirectory, OxygenDBConfig.defaultConfig());

    oxygenDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");
    final ODatabaseSession databaseDocumentTx = oxygenDB.open(DB_NAME, "admin", "admin");

    OMurmurHash3HashFunction<Integer> murmurHash3HashFunction =
        new OMurmurHash3HashFunction<Integer>(OIntegerSerializer.INSTANCE);

    localHashTable =
        new OLocalHashTableV3<>(
            "localHashTableTest",
            ".imc",
            ".tsc",
            ".obf",
            ".nbh",
            (OAbstractPaginatedStorage)
                ((ODatabaseSessionInternal) databaseDocumentTx).getStorage());

    atomicOperationsManager =
        ((OAbstractPaginatedStorage) ((ODatabaseSessionInternal) databaseDocumentTx).getStorage())
            .getAtomicOperationsManager();
    atomicOperationsManager.executeInsideAtomicOperation(
        null,
        atomicOperation ->
            localHashTable.create(
                atomicOperation,
                OIntegerSerializer.INSTANCE,
                OBinarySerializerFactory.getInstance().getObjectSerializer(OType.STRING),
                null,
                null,
                murmurHash3HashFunction,
                true));
  }

  @After
  public void after() {
    oxygenDB.drop(DB_NAME);
    oxygenDB.close();
  }
}
