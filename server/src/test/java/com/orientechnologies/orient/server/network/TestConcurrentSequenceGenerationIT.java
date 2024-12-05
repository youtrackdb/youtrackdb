package com.orientechnologies.orient.server.network;

import static org.junit.Assert.assertNotNull;

import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
import java.util.ArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestConcurrentSequenceGenerationIT {

  static final int THREADS = 20;
  static final int RECORDS = 100;
  private OServer server;
  private YouTrackDB youTrackDB;

  @Before
  public void before() throws Exception {
    server = new OServer(false);
    server.startup(getClass().getResourceAsStream("orientdb-server-config.xml"));
    server.activate();
    youTrackDB = new YouTrackDB("remote:localhost", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        TestConcurrentSequenceGenerationIT.class.getSimpleName());
    YTDatabaseSession databaseSession =
        youTrackDB.open(TestConcurrentSequenceGenerationIT.class.getSimpleName(), "admin", "admin");
    databaseSession.execute(
        "sql",
        """
            CREATE CLASS TestSequence EXTENDS V;
             CREATE SEQUENCE TestSequenceIdSequence TYPE ORDERED;
            CREATE PROPERTY TestSequence.id LONG (MANDATORY TRUE, default\
             "sequence('TestSequenceIdSequence').next()");
            CREATE INDEX TestSequence_id_index ON TestSequence (id BY VALUE) UNIQUE;""");
    databaseSession.close();
  }

  @Test
  public void test() throws Exception {

    try (var pool = youTrackDB.cachedPool(TestConcurrentSequenceGenerationIT.class.getSimpleName(),
        "admin", "admin")) {
      var executorService = Executors.newFixedThreadPool(THREADS);
      var futures = new ArrayList<Future<Object>>();

      for (int i = 0; i < THREADS; i++) {
        var future =
            executorService.submit(
                () -> {
                  try (YTDatabaseSession db = pool.acquire()) {
                    for (int j = 0; j < RECORDS; j++) {
                      db.executeInTx(() -> {
                        Vertex vert = db.newVertex("TestSequence");
                        assertNotNull(vert.getProperty("id"));
                        db.save(vert);
                      });
                    }
                  }

                  return null;
                });
        futures.add(future);
      }

      for (var future : futures) {
        future.get();
      }

      executorService.shutdown();
    }
  }

  @After
  public void after() {
    youTrackDB.drop(TestConcurrentSequenceGenerationIT.class.getSimpleName());
    youTrackDB.close();
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    YouTrackDBManager.instance().startup();
  }
}
