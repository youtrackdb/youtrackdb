/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.common.concur.YTNeedRetryException;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.util.concurrent.atomic.AtomicLong;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class ConcurrentUpdatesTest extends DocumentDBBaseTest {

  private static final int OPTIMISTIC_CYCLES = 100;
  private static final int PESSIMISTIC_CYCLES = 100;
  private static final int THREADS = 10;

  private final AtomicLong counter = new AtomicLong();
  private final AtomicLong totalRetries = new AtomicLong();

  @Parameters(value = "remote")
  public ConcurrentUpdatesTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  class OptimisticUpdateField implements Runnable {

    YTRID rid1;
    YTRID rid2;
    String fieldValue = null;
    String threadName;

    public OptimisticUpdateField(YTRID iRid1, YTRID iRid2, String iThreadName) {
      super();
      rid1 = iRid1;
      rid2 = iRid2;
      threadName = iThreadName;
    }

    @Override
    public void run() {
      try {
        YTDatabaseSessionInternal db = acquireSession();
        for (int i = 0; i < OPTIMISTIC_CYCLES; i++) {
          int retries = 0;
          while (true) {
            retries++;
            try {
              db.begin();

              YTDocument vDoc1 = db.load(rid1);
              vDoc1.field(threadName, vDoc1.field(threadName) + ";" + i);
              vDoc1.save();

              YTDocument vDoc2 = db.load(rid2);
              vDoc2.field(threadName, vDoc2.field(threadName) + ";" + i);
              vDoc2.save();

              db.commit();

              counter.incrementAndGet();
              totalRetries.addAndGet(retries);
              break;
            } catch (YTNeedRetryException e) {
              Thread.sleep(retries * 10L);
            }
          }
          fieldValue += ";" + i;
        }

      } catch (Throwable e) {
        throw new IllegalStateException(e);
      }
    }
  }

  class PessimisticUpdate implements Runnable {

    YTRID rid;
    String threadName;
    boolean lock;

    public PessimisticUpdate(YTRID iRid, String iThreadName, boolean iLock) {
      super();

      rid = iRid;
      threadName = iThreadName;
      lock = iLock;
    }

    @Override
    public void run() {
      try {
        YTDatabaseSessionInternal db = acquireSession();

        for (int i = 0; i < PESSIMISTIC_CYCLES; i++) {
          String cmd = "update " + rid + " set total = total + 1";
          if (lock) {
            cmd += " lock record";
          }

          int retries = 0;
          while (true) {
            try {
              retries++;
              db.begin();
              db.command(cmd).close();
              db.commit();
              counter.incrementAndGet();

              if (retries % 10 == 0) {
                System.out.println(retries + " retries for thread " + threadName);
              }

              break;

            } catch (YTNeedRetryException e) {
              if (lock) {
                Assert.fail(YTNeedRetryException.class.getSimpleName() + " was encountered");
              }
            }
          }
        }
      } catch (RuntimeException e) {
        e.printStackTrace();
        throw e;
      }
    }
  }

  @Test
  public void concurrentOptimisticUpdates() throws Exception {
    counter.set(0);

    YTDatabaseSessionInternal database = acquireSession();

    YTDocument doc1 = database.newInstance();
    doc1.field("INIT", "ok");
    database.begin();
    database.save(doc1);
    database.commit();

    YTRID rid1 = doc1.getIdentity();

    YTDocument doc2 = database.newInstance();
    doc2.field("INIT", "ok");

    database.begin();
    database.save(doc2);
    database.commit();

    YTRID rid2 = doc2.getIdentity();

    OptimisticUpdateField[] ops = new OptimisticUpdateField[THREADS];
    for (int i = 0; i < THREADS; ++i) {
      ops[i] = new OptimisticUpdateField(rid1, rid2, "thread" + i);
    }

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i) {
      threads[i] = new Thread(ops[i], "ConcurrentTest" + i);
    }

    for (int i = 0; i < THREADS; ++i) {
      threads[i].start();
    }

    for (int i = 0; i < THREADS; ++i) {
      threads[i].join();
    }

    Assert.assertEquals(counter.get(), OPTIMISTIC_CYCLES * THREADS);

    doc1 = database.load(rid1);

    for (int i = 0; i < THREADS; ++i) {
      Assert.assertEquals(doc1.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);
    }

    doc1.toJSON();

    doc2 = database.load(rid2);

    for (int i = 0; i < THREADS; ++i) {
      Assert.assertEquals(doc2.field(ops[i].threadName), ops[i].fieldValue, ops[i].threadName);
    }

    doc2.toJSON();
    System.out.println(doc2.toJSON());

    database.close();
  }

  @Test(enabled = false)
  public void concurrentPessimisticSQLUpdates() throws Exception {
    sqlUpdate(true);
  }

  @Test
  public void concurrentOptimisticSQLUpdates() throws Exception {
    sqlUpdate(false);
  }

  protected void sqlUpdate(boolean lock) throws InterruptedException {
    counter.set(0);

    YTDatabaseSessionInternal database = acquireSession();
    YTDocument doc1 = database.newInstance();
    doc1.field("total", 0);

    database.begin();
    database.save(doc1);
    database.commit();

    YTRID rid1 = doc1.getIdentity();

    PessimisticUpdate[] ops = new PessimisticUpdate[THREADS];
    for (int i = 0; i < THREADS; ++i) {
      ops[i] = new PessimisticUpdate(rid1, "thread" + i, lock);
    }

    Thread[] threads = new Thread[THREADS];
    for (int i = 0; i < THREADS; ++i) {
      threads[i] = new Thread(ops[i], "ConcurrentTest" + i);
    }

    for (int i = 0; i < THREADS; ++i) {
      threads[i].start();
    }

    for (int i = 0; i < THREADS; ++i) {
      threads[i].join();
    }

    Assert.assertEquals(counter.get(), PESSIMISTIC_CYCLES * THREADS);

    doc1 = database.load(rid1);
    Assert.assertEquals(doc1.<Object>field("total"), PESSIMISTIC_CYCLES * THREADS);

    database.close();
  }
}
