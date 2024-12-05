/*
 *
 *  *  Copyright YouTrackDB
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */

package com.orientechnologies.orient.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.orient.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.io.File;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class FreezeAndRecordInsertAtomicityTest {

  private static final String URL;
  private static final int THREADS = Runtime.getRuntime().availableProcessors() * 2;
  private static final int ITERATIONS = 100;

  static {
    String buildDirectory = System.getProperty("buildDirectory");
    if (buildDirectory == null) {
      buildDirectory = "./target";
    }

    URL =
        "plocal:"
            + buildDirectory
            + File.separator
            + FreezeAndRecordInsertAtomicityTest.class.getSimpleName();
  }

  private Random random;
  private YTDatabaseDocumentTx db;
  private ExecutorService executorService;
  private CountDownLatch countDownLatch;

  @Before
  public void before() {
    final long seed = System.currentTimeMillis();
    System.out.println(FreezeAndRecordInsertAtomicityTest.class.getSimpleName() + " seed: " + seed);
    random = new Random(seed);

    db = new YTDatabaseDocumentTx(URL);
    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }
    db.create();
    db.getMetadata()
        .getSchema()
        .createClass("Person")
        .createProperty(db, "name", YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    executorService = Executors.newFixedThreadPool(THREADS);

    countDownLatch = new CountDownLatch(THREADS);
  }

  @After
  public void after() throws InterruptedException {
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));

    db.drop();
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    final Set<Future<?>> futures = new HashSet<Future<?>>();

    for (int i = 0; i < THREADS; ++i) {
      final int thread = i;

      futures.add(
          executorService.submit(
              () -> {
                try {
                  final YTDatabaseSessionInternal db = new YTDatabaseDocumentTx(URL);
                  db.open("admin", "admin");
                  final OIndex index =
                      db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.name");

                  for (int i1 = 0; i1 < ITERATIONS; ++i1) {
                    switch (random.nextInt(2)) {
                      case 0:
                        var val = i1;
                        db.executeInTx(
                            () ->
                                db.<YTEntityImpl>newInstance("Person")
                                    .field("name", "name-" + thread + "-" + val)
                                    .save());
                        break;

                      case 1:
                        db.freeze();
                        try {
                          for (YTEntityImpl document : db.browseClass("Person")) {
                            try (Stream<YTRID> rids =
                                index.getInternal().getRids(db, document.field("name"))) {
                              assertEquals(document.getIdentity(), rids.findFirst().orElse(null));
                            }
                          }
                        } finally {
                          db.release();
                        }

                        break;
                    }
                  }
                } catch (RuntimeException | Error e) {
                  e.printStackTrace();
                  throw e;
                } finally {
                  countDownLatch.countDown();
                }
              }));
    }

    countDownLatch.await();

    for (Future<?> future : futures) {
      future.get(); // propagate exceptions, if there are any
    }
  }
}
