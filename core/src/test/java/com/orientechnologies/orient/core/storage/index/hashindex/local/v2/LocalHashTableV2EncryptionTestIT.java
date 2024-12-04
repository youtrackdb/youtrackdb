package com.orientechnologies.orient.core.storage.index.hashindex.local.v2;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.common.serialization.types.OIntegerSerializer;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.encryption.OEncryption;
import com.orientechnologies.orient.core.encryption.OEncryptionFactory;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.serialization.serializer.binary.OBinarySerializerFactory;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import com.orientechnologies.orient.core.storage.index.hashindex.local.OSHA256HashFunction;
import java.io.File;
import org.junit.After;
import org.junit.Before;

public class LocalHashTableV2EncryptionTestIT extends LocalHashTableV2Base {

  private YouTrackDB youTrackDB;

  private static final String DB_NAME = "localHashTableEncryptionTest";

  @Before
  public void before() throws Exception {
    String buildDirectory = System.getProperty("buildDirectory", ".");
    final File dbDirectory = new File(buildDirectory, DB_NAME);

    OFileUtils.deleteRecursively(dbDirectory);
    final YouTrackDBConfig config = YouTrackDBConfig.builder().build();
    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, config);
    youTrackDB.execute(
        "create database " + DB_NAME + " plocal users ( admin identified by 'admin' role admin)");

    var databaseDocumentTx = youTrackDB.open(DB_NAME, "admin", "admin");
    storage =
        (OAbstractPaginatedStorage) ((YTDatabaseSessionInternal) databaseDocumentTx).getStorage();

    final OEncryption encryption =
        OEncryptionFactory.INSTANCE.getEncryption("aes/gcm", "T1JJRU5UREJfSVNfQ09PTA==");

    OSHA256HashFunction<Integer> SHA256HashFunction =
        new OSHA256HashFunction<>(OIntegerSerializer.INSTANCE);

    localHashTable =
        new LocalHashTableV2<>(
            "localHashTableEncryptionTest", ".imc", ".tsc", ".obf", ".nbh", storage);

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
                    encryption,
                    SHA256HashFunction,
                    true));
  }

  @After
  public void after() {
    youTrackDB.drop(DB_NAME);
    youTrackDB.close();
  }
}
