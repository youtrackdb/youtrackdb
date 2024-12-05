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
package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryMonitor;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.OLiveResultListener;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLiveQueryTest {

  private YouTrackDB odb;
  private YTDatabaseSessionInternal db;

  @Before
  public void before() {
    odb = new YouTrackDB("memory:", YouTrackDBConfig.defaultConfig());
    odb.execute(
        "create database OLiveQueryTest memory users ( admin identified by 'admin' role admin,"
            + " reader identified by 'reader' role reader)");
    db = (YTDatabaseSessionInternal) odb.open("OLiveQueryTest", "admin", "admin");
  }

  @After
  public void after() {
    db.close();
    odb.drop("OLiveQueryTest");
    odb.close();
  }

  class MyLiveQueryListener implements OLiveResultListener, YTLiveQueryResultListener {

    public CountDownLatch latch;

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<ORecordOperation> ops = new ArrayList<ORecordOperation>();
    public List<YTResult> created = new ArrayList<YTResult>();

    @Override
    public void onLiveResult(int iLiveToken, ORecordOperation iOp) throws YTException {
      ops.add(iOp);
      latch.countDown();
    }

    @Override
    public void onError(int iLiveToken) {
    }

    @Override
    public void onUnsubscribe(int iLiveToken) {
    }

    @Override
    public void onCreate(YTDatabaseSession database, YTResult data) {
      created.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(YTDatabaseSession database, YTResult before, YTResult after) {
    }

    @Override
    public void onDelete(YTDatabaseSession database, YTResult data) {
    }

    @Override
    public void onError(YTDatabaseSession database, YTException exception) {
    }

    @Override
    public void onEnd(YTDatabaseSession database) {
    }
  }

  @Test
  public void testLiveInsert() throws InterruptedException {

    db.getMetadata().getSchema().createClass("test");
    db.getMetadata().getSchema().createClass("test2");
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

    YTLiveQueryMonitor tokens = db.live("live select from test", listener);
    Integer token = tokens.getMonitorId();
    Assert.assertNotNull(token);

    db.begin();
    db.command("insert into test set name = 'foo', surname = 'bar'").close();
    db.command("insert into test set name = 'foo', surname = 'baz'").close();
    db.command("insert into test2 set name = 'foo'").close();
    db.commit();

    Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

    tokens.unSubscribe();

    db.begin();
    db.command("insert into test set name = 'foo', surname = 'bax'").close();
    db.command("insert into test2 set name = 'foo'").close();
    db.command("insert into test set name = 'foo', surname = 'baz'").close();
    db.commit();

    Assert.assertEquals(listener.created.size(), 2);
    for (YTResult res : listener.created) {
      Assert.assertEquals(res.getProperty("name"), "foo");
    }
  }

  @Test
  public void testLiveInsertOnCluster() {

    YTClass clazz = db.getMetadata().getSchema().createClass("test");

    int defaultCluster = clazz.getDefaultClusterId();
    final OStorage storage = db.getStorage();

    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(1));

    db.live("live select from cluster:" + storage.getClusterNameById(defaultCluster), listener);

    db.begin();
    db.command(
            "insert into cluster:"
                + storage.getClusterNameById(defaultCluster)
                + " set name = 'foo', surname = 'bar'")
        .close();
    db.commit();

    try {
      Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
    Assert.assertEquals(listener.created.size(), 1);
    for (YTResult doc : listener.created) {
      Assert.assertEquals(doc.getProperty("name"), "foo");
      YTRID rid = doc.getProperty("@rid");
      Assert.assertNotNull(rid);
      Assert.assertTrue(rid.getClusterPosition() >= 0);
    }
  }

  @Test
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {

    YTSchema schema = db.getMetadata().getSchema();
    YTClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 2;
    YTResultSet query = db.query("select from OUSer where name = 'reader'");

    final YTIdentifiable reader = query.next().getIdentity().get();
    final YTIdentifiable current = db.getUser().getIdentity(db);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(2);
    Future<Integer> future =
        executorService.submit(
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                YTDatabaseSession otherDb = odb.open("OLiveQueryTest", "reader", "reader");

                final AtomicInteger integer = new AtomicInteger(0);
                try {
                  otherDb.live(
                      "live select from test",
                      new YTLiveQueryResultListener() {

                        @Override
                        public void onCreate(YTDatabaseSession database, YTResult data) {
                          integer.incrementAndGet();
                          dataArrived.countDown();
                        }

                        @Override
                        public void onUpdate(
                            YTDatabaseSession database, YTResult before, YTResult after) {
                        }

                        @Override
                        public void onDelete(YTDatabaseSession database, YTResult data) {
                        }

                        @Override
                        public void onError(YTDatabaseSession database, YTException exception) {
                        }

                        @Override
                        public void onEnd(YTDatabaseSession database) {
                        }
                      });
                } catch (RuntimeException e) {
                  e.printStackTrace();
                }

                latch.countDown();
                Assert.assertTrue(dataArrived.await(1, TimeUnit.MINUTES));
                return integer.get();
              }
            });

    latch.await();

    db.begin();
    db.command("insert into test set name = 'foo', surname = 'bar'").close();

    db.command(
            "insert into test set name = 'foo', surname = 'bar', _allow=?",
            new ArrayList<YTIdentifiable>() {
              {
                add(current);
                add(reader);
              }
            })
        .close();
    db.commit();

    Integer integer = future.get();
    Assert.assertEquals(integer.intValue(), liveMatch);
  }
}
