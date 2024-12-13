/*
 *
 *
 *  *
 *  * Licensed under the Apache License, Version 2.0 (the "License");
 *  * you may not use this file except in compliance with the License.
 *  * You may obtain a copy of the License at
 *  *
 *  *      http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  * Unless required by applicable law or agreed to in writing, software
 *  * distributed under the License is distributed on an "AS IS" BASIS,
 *  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  * See the License for the specific language governing permissions and
 *  * limitations under the License.
 *
 */

package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.engine.local.EngineLocalPaginated;
import com.jetbrains.youtrack.db.internal.core.engine.memory.EngineMemory;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.Test;

/**
 *
 */
public class LuceneInsertMultithreadTest {

  private static final int THREADS = 10;
  private static final int RTHREADS = 1;
  private static final int CYCLE = 100;
  private static String buildDirectory;
  private static final String dbName;
  private static final DatabaseType databaseType;
  private static final YouTrackDB YOUTRACKDB;

  static {
    System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    String config = System.getProperty("youtrackdb.test.env");

    String storageType;
    if ("ci".equals(config) || "release".equals(config)) {
      storageType = EngineLocalPaginated.NAME;
      databaseType = DatabaseType.PLOCAL;
    } else {
      storageType = EngineMemory.NAME;
      databaseType = DatabaseType.MEMORY;
    }

    dbName = "multiThread";
    YOUTRACKDB = new YouTrackDBImpl(storageType + ":" + buildDirectory,
        YouTrackDBConfig.defaultConfig());
  }

  public LuceneInsertMultithreadTest() {
    super();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {
    if (YOUTRACKDB.exists(dbName)) {
      YOUTRACKDB.drop(dbName);
    }
    YOUTRACKDB.execute(
        "create database ? " + databaseType + " users(admin identified by 'admin' role admin)",
        dbName);
    Schema schema;
    try (DatabaseSessionInternal databaseDocumentTx = (DatabaseSessionInternal) YOUTRACKDB.open(
        dbName, "admin", "admin")) {
      schema = databaseDocumentTx.getMetadata().getSchema();

      if (schema.getClass("City") == null) {
        SchemaClass oClass = schema.createClass("City");

        oClass.createProperty(databaseDocumentTx, "name", PropertyType.STRING);
        oClass.createIndex(databaseDocumentTx, "City.name", "FULLTEXT", null, null, "LUCENE",
            new String[]{"name"});
      }

      Thread[] threads = new Thread[THREADS + RTHREADS];
      for (int i = 0; i < THREADS; ++i) {
        threads[i] = new Thread(new LuceneInsertThread(CYCLE), "ConcurrentWriteTest" + i);
      }

      for (int i = THREADS; i < THREADS + RTHREADS; ++i) {
        threads[i] = new Thread(new LuceneReadThread(CYCLE), "ConcurrentReadTest" + i);
      }

      for (int i = 0; i < THREADS + RTHREADS; ++i) {
        threads[i].start();
      }

      for (int i = 0; i < THREADS + RTHREADS; ++i) {
        threads[i].join();
      }

      Index idx = databaseDocumentTx.getClassInternal("City")
          .getClassIndex(databaseDocumentTx, "City.name");

      databaseDocumentTx.begin();
      Assertions.assertThat(idx.getInternal().size(databaseDocumentTx))
          .isEqualTo(THREADS * CYCLE);
      databaseDocumentTx.commit();
    }
    YOUTRACKDB.drop(dbName);
  }

  public static class LuceneInsertThread implements Runnable {

    private final int cycle;

    private LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      try (DatabaseSession db = YOUTRACKDB.open(dbName, "admin", "admin")) {
        db.begin();
        for (int i = 0; i < cycle; i++) {
          EntityImpl doc = new EntityImpl("City");

          doc.field("name", "Rome");

          db.begin();
          db.save(doc);
          db.commit();
          int commitBuf = 500;
          if (i % commitBuf == 0) {
            db.commit();
            db.begin();
          }
        }
        db.commit();
      }
    }
  }

  public static class LuceneReadThread implements Runnable {

    private final int cycle;

    private LuceneReadThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {
      Schema schema;
      try (DatabaseSessionInternal databaseDocumentTx = (DatabaseSessionInternal) YOUTRACKDB.open(
          dbName, "admin", "admin")) {
        schema = databaseDocumentTx.getMetadata().getSchema();

        Index idx = databaseDocumentTx.getClassInternal("City")
            .getClassIndex(databaseDocumentTx, "City.name");

        for (int i = 0; i < cycle; i++) {
          try (Stream<RID> stream = idx.getInternal()
              .getRids(databaseDocumentTx, "Rome")) {
            //noinspection ResultOfMethodCallIgnored
            stream.collect(Collectors.toList());
          }
        }
      }
    }
  }
}
