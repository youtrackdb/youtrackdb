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

package com.orientechnologies.lucene.test;

import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseType;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.engine.local.OEngineLocalPaginated;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
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
  private static final ODatabaseType databaseType;
  private static final OxygenDB OXYGEN_DB;

  static {
    System.getProperty("buildDirectory", ".");
    if (buildDirectory == null) {
      buildDirectory = ".";
    }

    String config = System.getProperty("oxygendb.test.env");

    String storageType;
    if ("ci".equals(config) || "release".equals(config)) {
      storageType = OEngineLocalPaginated.NAME;
      databaseType = ODatabaseType.PLOCAL;
    } else {
      storageType = OEngineMemory.NAME;
      databaseType = ODatabaseType.MEMORY;
    }

    dbName = "multiThread";
    OXYGEN_DB = new OxygenDB(storageType + ":" + buildDirectory, OxygenDBConfig.defaultConfig());
  }

  public LuceneInsertMultithreadTest() {
    super();
  }

  @Test
  public void testConcurrentInsertWithIndex() throws Exception {
    if (OXYGEN_DB.exists(dbName)) {
      OXYGEN_DB.drop(dbName);
    }
    OXYGEN_DB.execute(
        "create database ? " + databaseType + " users(admin identified by 'admin' role admin)",
        dbName);
    OSchema schema;
    try (ODatabaseSessionInternal databaseDocumentTx = (ODatabaseSessionInternal) OXYGEN_DB.open(
        dbName, "admin", "admin")) {
      schema = databaseDocumentTx.getMetadata().getSchema();

      if (schema.getClass("City") == null) {
        OClass oClass = schema.createClass("City");

        oClass.createProperty(databaseDocumentTx, "name", OType.STRING);
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

      OIndex idx = schema.getClass("City").getClassIndex(databaseDocumentTx, "City.name");

      databaseDocumentTx.begin();
      Assertions.assertThat(idx.getInternal().size(databaseDocumentTx))
          .isEqualTo(THREADS * CYCLE);
      databaseDocumentTx.commit();
    }
    OXYGEN_DB.drop(dbName);
  }

  public static class LuceneInsertThread implements Runnable {

    private final int cycle;

    private LuceneInsertThread(int cycle) {
      this.cycle = cycle;
    }

    @Override
    public void run() {

      try (ODatabaseSession db = OXYGEN_DB.open(dbName, "admin", "admin")) {
        db.begin();
        for (int i = 0; i < cycle; i++) {
          ODocument doc = new ODocument("City");

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
      OSchema schema;
      try (ODatabaseSessionInternal databaseDocumentTx = (ODatabaseSessionInternal) OXYGEN_DB.open(
          dbName, "admin", "admin")) {
        schema = databaseDocumentTx.getMetadata().getSchema();

        OIndex idx = schema.getClass("City").getClassIndex(databaseDocumentTx, "City.name");

        for (int i = 0; i < cycle; i++) {
          try (Stream<ORID> stream = idx.getInternal()
              .getRids(databaseDocumentTx, "Rome")) {
            //noinspection ResultOfMethodCallIgnored
            stream.collect(Collectors.toList());
          }
        }
      }
    }
  }
}
