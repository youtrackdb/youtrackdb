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

import com.orientechnologies.orient.client.remote.OStorageRemote;
import com.orientechnologies.orient.core.db.ODatabase;
import com.orientechnologies.orient.core.db.ODatabaseInternal;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
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
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @author Michael Hiess
 */
public class MultipleDBTest extends DocumentDBBaseTest {

  public MultipleDBTest() {}

  @Parameters(value = "remote")
  public MultipleDBTest(boolean remote) {
    super(remote);
  }

  @Test
  public void testObjectMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<>());

    Set<Future<Void>> threads = new HashSet<>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    for (int i = 0; i < dbs; i++) {
      var dbName = this.dbName + i;
      Callable<Void> t =
          () -> {
            dropDatabase(dbName);
            createDatabase(dbName);
            try {
              var db = createSessionInstance(dbName);

              db.set(ODatabase.ATTRIBUTES.MINIMUMCLUSTERS, 1);
              db.getMetadata().getSchema().getOrCreateClass("DummyObject");

              long start = System.currentTimeMillis();
              for (int j = 0; j < operations_write; j++) {
                var dummy = db.newInstance("DummyObject");
                dummy.setProperty("name", "name" + j);

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
              // System.out.println(time);
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
              // System.out.println(time);
              times.add(time);

              db.close();

            } finally {
              dropDatabase(dbName);
            }
            return null;
          };

      threads.add(executorService.submit(t));
    }

    for (Future<Void> future : threads) {
      future.get();
    }
  }

  @Test
  public void testDocumentMultipleDBsThreaded() throws Exception {
    final int operations_write = 1000;
    final int operations_read = 1;
    final int dbs = 10;

    final Set<String> times = Collections.newSetFromMap(new ConcurrentHashMap<>());

    Set<Future<Void>> results = new HashSet<>();
    ExecutorService executorService = Executors.newFixedThreadPool(4);
    for (int i = 0; i < dbs; i++) {
      var dbName = this.dbName + i;
      Callable<Void> t =
          () -> {
            dropDatabase(dbName);
            createDatabase(dbName);

            try (var db = createSessionInstance(dbName)) {
              db.getMetadata().getSchema().createClass("DummyObject", 1);

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
              dropDatabase(dbName);
            }
            return null;
          };

      results.add(executorService.submit(t));
    }

    for (Future<Void> future : results) {
      future.get();
    }
  }

  private String getDbId(ODatabaseInternal<?> tx) {
    if (tx.getStorage() instanceof OStorageRemote) {
      return tx.getURL() + " - sessionId: " + ((OStorageRemote) tx.getStorage()).getSessionId();
    } else {
      return tx.getURL();
    }
  }
}
