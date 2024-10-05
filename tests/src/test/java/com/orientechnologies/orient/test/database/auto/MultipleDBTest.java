/**
 * Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
 *
 * <p>Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file
 * except in compliance with the License. You may obtain a copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.client.db.ODatabaseHelper;
import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseDocumentInternal;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.db.document.ODatabaseDocumentTx;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import com.orientechnologies.orient.object.db.OObjectDatabaseTxInternal;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Michael Hiess
 */
public class MultipleDBTest extends DocumentDBBaseTest {

  @Parameters(value = "url")
  public MultipleDBTest(@Optional String url) {
    super(url, "-");
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {}

  @BeforeMethod
  @Override
  public void beforeMethod() throws Exception {}

  @AfterMethod
  @Override
  public void afterMethod() throws Exception {}

  @AfterClass
  @Override
  public void afterClass() throws Exception {}

  @Test
  public void testObjectMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> threads = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String dbUrl = url + i;

      Callable<Void> t =
          new Callable<Void>() {

            @Override
            public Void call() throws InterruptedException, IOException {
              OObjectDatabaseTxInternal db = new OObjectDatabaseTxInternal(dbUrl);

              ODatabaseHelper.deleteDatabase(db, getStorageType());
              ODatabaseHelper.createDatabase(db, dbUrl, getStorageType());
              try {
                if (db.isClosed()) {
                  db.open("admin", "admin");
                }

                db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1);
                db.getMetadata().getSchema().getOrCreateClass("DummyObject");
                db.getEntityManager().registerEntityClass(DummyObject.class);

                long start = System.currentTimeMillis();
                for (int j = 0; j < operations_write; j++) {
                  DummyObject dummy = new DummyObject("name" + j);

                  db.begin();
                  dummy = db.save(dummy);
                  db.commit();

                  Assert.assertEquals(
                      ((ORID) dummy.getId()).getClusterPosition(), j, "RID was " + dummy.getId());
                }
                long end = System.currentTimeMillis();

                String time =
                    "("
                        + getDbId(db)
                        + ") "
                        + "Executed operations (WRITE) in: "
                        + (end - start)
                        + " ms";
                // System.out.println(time);
                times.add(time);

                start = System.currentTimeMillis();
                for (int j = 0; j < operations_read; j++) {
                  List<DummyObject> l =
                      db.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
                  Assert.assertEquals(l.size(), operations_write);

                  // if ((j + 1) % 20000 == 0) {
                  // System.out.println("(" + getDbId(tx) + ") " + "Operations (READ) executed: " +
                  // j + 1);
                  // }
                }
                end = System.currentTimeMillis();

                time =
                    "("
                        + getDbId(db)
                        + ") "
                        + "Executed operations (READ) in: "
                        + (end - start)
                        + " ms";
                // System.out.println(time);
                times.add(time);

                db.close();

              } finally {
                // System.out.println("(" + getDbId(tx) + ") " + "Dropping");
                // System.out.flush();
                ODatabaseHelper.deleteDatabase(db, getStorageType());
                // System.out.println("(" + getDbId(tx) + ") " + "Dropped");
                // System.out.flush();
              }
              return null;
            }
          };

      threads.add(executorService.submit(t));
    }

    for (Future future : threads) {
      future.get();
    }

    // System.out.println("Test testObjectMultipleDBsThreaded ended");
  }

  @Test
  public void testDocumentMultipleDBsThreaded() throws Exception {
    if (url.startsWith("remote")) {
      ODatabaseDocumentTx.closeAll();
    }
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<String, Boolean>());

    Set<Future> results = new HashSet<Future>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);

    for (int i = 0; i < dbs; i++) {

      final String dbUrl = url + i;

      Callable<Void> t =
          () -> {
            ODatabaseDocumentInternal db = new ODatabaseDocumentTx(dbUrl);

            ODatabaseHelper.deleteDatabase(db, getStorageType());
            ODatabaseHelper.createDatabase(db, dbUrl, getStorageType());

            try {
              if (db.isClosed()) {
                db.open("admin", "admin");
              }

              db.getMetadata().getSchema().createClass("DummyObject", 1, null);

              long start = System.currentTimeMillis();
              for (int j = 0; j < operations_write; j++) {

                ODocument dummy = new ODocument("DummyObject");
                dummy.field("name", "name" + j);

                db.begin();
                dummy = db.save(dummy);
                db.commit();

                Assert.assertEquals(
                    dummy.getIdentity().getClusterPosition(), j, "RID was " + dummy.getIdentity());
              }
              long end = System.currentTimeMillis();

              String time =
                  "("
                      + getDbId(db)
                      + ") "
                      + "Executed operations (WRITE) in: "
                      + (end - start)
                      + " ms";
              times.add(time);

              start = System.currentTimeMillis();
              for (int j = 0; j < operations_read; j++) {
                List<DummyObject> l =
                    db.query(new OSQLSynchQuery<DummyObject>(" select * from DummyObject "));
                Assert.assertEquals(l.size(), operations_write);
              }
              end = System.currentTimeMillis();

              time =
                  "("
                      + getDbId(db)
                      + ") "
                      + "Executed operations (READ) in: "
                      + (end - start)
                      + " ms";
              times.add(time);

            } finally {
              db.close();

              ODatabaseHelper.deleteDatabase(db, getStorageType());
            }
            return null;
          };

      results.add(executorService.submit(t));
    }

    for (Future future : results) {
      future.get();
    }
  }

  private String getDbId(ODatabaseInternal tx) {
    if (tx.getStorage() instanceof OStorageRemote) {
      return tx.getURL() + " - sessionId: " + ((OStorageRemote) tx.getStorage()).getSessionId();
    } else {
      return tx.getURL();
    }
  }
}
