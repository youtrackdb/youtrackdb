package com.orientechnologies.orient.core.storage.cluster.v2;

import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.storage.cluster.LocalPaginatedClusterAbstract;
import com.orientechnologies.orient.core.storage.impl.local.OAbstractPaginatedStorage;
import java.io.File;
import java.io.IOException;
import org.junit.BeforeClass;

public class LocalPaginatedClusterV2TestIT extends LocalPaginatedClusterAbstract {

  @BeforeClass
  public static void beforeClass() throws IOException {
    buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null || buildDirectory.isEmpty()) {
      buildDirectory = ".";
    }

    buildDirectory += File.separator + LocalPaginatedClusterV2TestIT.class.getSimpleName();
    OFileUtils.deleteRecursively(new File(buildDirectory));

    dbName = "clusterTest";

    final YouTrackDBConfig config = YouTrackDBConfig.defaultConfig();
    youTrackDB = new YouTrackDB("plocal:" + buildDirectory, config);
    youTrackDB.execute(
        "create database " + dbName + " plocal users ( admin identified by 'admin' role admin)");

    databaseDocumentTx = (YTDatabaseSessionInternal) youTrackDB.open(dbName, "admin", "admin");

    storage = (OAbstractPaginatedStorage) databaseDocumentTx.getStorage();

    paginatedCluster = new OPaginatedClusterV2("paginatedClusterTest", storage);
    paginatedCluster.configure(42, "paginatedClusterTest");
    storage
        .getAtomicOperationsManager()
        .executeInsideAtomicOperation(
            null, atomicOperation -> paginatedCluster.create(atomicOperation));
  }
}
