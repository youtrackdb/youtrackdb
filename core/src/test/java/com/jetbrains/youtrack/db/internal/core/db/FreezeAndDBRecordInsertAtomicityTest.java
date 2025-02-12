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

package com.jetbrains.youtrack.db.internal.core.db;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.DatabaseType;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class FreezeAndDBRecordInsertAtomicityTest extends DbTestBase {

  private static final int THREADS = Runtime.getRuntime().availableProcessors() << 1;
  private static final int ITERATIONS = 100;

  private Random random;
  private ExecutorService executorService;
  private CountDownLatch countDownLatch;

  @Override
  protected DatabaseType calculateDbType() {
    return DatabaseType.PLOCAL;
  }

  @Before
  public void before() {
    final var seed = System.currentTimeMillis();
    System.out.println(
        FreezeAndDBRecordInsertAtomicityTest.class.getSimpleName() + " seed: " + seed);
    random = new Random(seed);

    session.getMetadata()
        .getSchema()
        .createClass("Person")
        .createProperty(session, "name", PropertyType.STRING)
        .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);

    executorService = Executors.newFixedThreadPool(THREADS);
    countDownLatch = new CountDownLatch(THREADS);
  }

  @After
  public void after() throws InterruptedException {
    executorService.shutdown();
    assertTrue(executorService.awaitTermination(5, TimeUnit.SECONDS));
  }

  @Test
  public void test() throws InterruptedException, ExecutionException {
    final Set<Future<?>> futures = new HashSet<Future<?>>();

    for (var i = 0; i < THREADS; ++i) {
      final var thread = i;

      futures.add(
          executorService.submit(
              () -> {
                try (final var db = openDatabase()) {
                  final var index =
                      db.getMetadata().getIndexManagerInternal().getIndex(db, "Person.name");
                  for (var i1 = 0; i1 < ITERATIONS; ++i1) {
                    switch (random.nextInt(2)) {
                      case 0:
                        var val = i1;
                        db.executeInTx(
                            () ->
                                db.newInstance("Person")
                                    .field("name", "name-" + thread + "-" + val)
                                    .save());
                        break;

                      case 1:
                        db.freeze();
                        try {
                          for (var document : db.browseClass("Person")) {
                            try (var rids =
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

    for (var future : futures) {
      future.get(); // propagate exceptions, if there are any
    }
  }
}
