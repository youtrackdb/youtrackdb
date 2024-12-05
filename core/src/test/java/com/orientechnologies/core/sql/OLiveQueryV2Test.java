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
package com.orientechnologies.core.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YTLiveQueryMonitor;
import com.orientechnologies.core.db.YTLiveQueryResultListener;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.db.document.YTDatabaseDocumentTx;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.query.OSQLSynchQuery;
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
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OLiveQueryV2Test {

  class MyLiveQueryListener implements YTLiveQueryResultListener {

    public CountDownLatch latch;

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<YTResult> ops = new ArrayList<YTResult>();

    @Override
    public void onCreate(YTDatabaseSession database, YTResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(YTDatabaseSession database, YTResult before, YTResult after) {
      ops.add(after);
      latch.countDown();
    }

    @Override
    public void onDelete(YTDatabaseSession database, YTResult data) {
      ops.add(data);
      latch.countDown();
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
    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx("memory:OLiveQueryV2Test");
    db.activateOnCurrentThread();
    db.create();
    try {
      db.getMetadata().getSchema().createClass("test");
      db.getMetadata().getSchema().createClass("test2");
      MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

      YTLiveQueryMonitor monitor = db.live("select from test", listener);
      Assert.assertNotNull(monitor);

      db.begin();
      db.command("insert into test set name = 'foo', surname = 'bar'").close();
      db.command("insert into test set name = 'foo', surname = 'baz'").close();
      db.command("insert into test2 set name = 'foo'").close();
      db.commit();

      Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

      monitor.unSubscribe();

      db.begin();
      db.command("insert into test set name = 'foo', surname = 'bax'").close();
      db.command("insert into test2 set name = 'foo'").close();
      db.command("insert into test set name = 'foo', surname = 'baz'").close();
      db.commit();

      Assert.assertEquals(2, listener.ops.size());
      for (YTResult doc : listener.ops) {
        Assert.assertEquals("test", doc.getProperty("@class"));
        Assert.assertEquals("foo", doc.getProperty("name"));
        YTRID rid = doc.getProperty("@rid");
        Assert.assertTrue(rid.isPersistent());
      }
    } finally {
      db.drop();
    }
  }

  @Test
  public void testLiveInsertOnCluster() {
    final YouTrackDB context =
        OCreateDatabaseUtil.createDatabase(
            "testLiveInsertOnCluster", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    try (YTDatabaseSessionInternal db =
        (YTDatabaseSessionInternal)
            context.open(
                "testLiveInsertOnCluster", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {

      YTClass clazz = db.getMetadata().getSchema().createClass("test");

      int defaultCluster = clazz.getDefaultClusterId();
      String clusterName = db.getStorage().getClusterNameById(defaultCluster);

      OLiveQueryV2Test.MyLiveQueryListener listener =
          new OLiveQueryV2Test.MyLiveQueryListener(new CountDownLatch(1));

      db.live(" select from cluster:" + clusterName, listener);

      db.begin();
      db.command("insert into cluster:" + clusterName + " set name = 'foo', surname = 'bar'");
      db.commit();

      try {
        Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(1, listener.ops.size());
      for (YTResult doc : listener.ops) {
        Assert.assertEquals("foo", doc.getProperty("name"));
        YTRID rid = doc.getProperty("@rid");
        Assert.assertTrue(rid.isPersistent());
        Assert.assertNotNull(rid);
      }
    }
  }

  @Test
  public void testLiveWithWhereCondition() {
    final YouTrackDB context =
        OCreateDatabaseUtil.createDatabase(
            "testLiveWithWhereCondition", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    try (YTDatabaseSessionInternal db =
        (YTDatabaseSessionInternal)
            context.open(
                "testLiveWithWhereCondition", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {

      db.getMetadata().getSchema().createClass("test");

      OLiveQueryV2Test.MyLiveQueryListener listener =
          new OLiveQueryV2Test.MyLiveQueryListener(new CountDownLatch(1));

      db.live("select from V where id = 1", listener);

      db.begin();
      db.command("insert into V set id = 1");
      db.commit();

      try {
        Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
      Assert.assertEquals(1, listener.ops.size());
      for (YTResult doc : listener.ops) {
        Assert.assertEquals(doc.getProperty("id"), Integer.valueOf(1));
        YTRID rid = doc.getProperty("@rid");
        Assert.assertTrue(rid.isPersistent());
        Assert.assertNotNull(rid);
      }
    }
  }

  @Test
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx("memory:OLiveQueryTest");
    db.activateOnCurrentThread();
    db.create();
    try {
      YTSchema schema = db.getMetadata().getSchema();
      YTClass oRestricted = schema.getClass("ORestricted");
      schema.createClass("test", oRestricted);

      int liveMatch = 2;
      List<YTEntityImpl> query =
          db.query(new OSQLSynchQuery("select from OUSer where name = 'reader'"));

      final YTIdentifiable reader = query.iterator().next().getIdentity();
      final YTIdentifiable current = db.getUser().getIdentity(db);

      ExecutorService executorService = Executors.newSingleThreadExecutor();

      final CountDownLatch latch = new CountDownLatch(1);
      final CountDownLatch dataArrived = new CountDownLatch(liveMatch);
      Future<Integer> future =
          executorService.submit(
              new Callable<Integer>() {
                @Override
                public Integer call() throws Exception {
                  YTDatabaseDocumentTx db = new YTDatabaseDocumentTx("memory:OLiveQueryTest");
                  db.open("reader", "reader");

                  final AtomicInteger integer = new AtomicInteger(0);
                  db.live(
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
                          integer.incrementAndGet();
                          dataArrived.countDown();
                        }

                        @Override
                        public void onDelete(YTDatabaseSession database, YTResult data) {
                          integer.incrementAndGet();
                          dataArrived.countDown();
                        }

                        @Override
                        public void onError(YTDatabaseSession database, YTException exception) {
                        }

                        @Override
                        public void onEnd(YTDatabaseSession database) {
                        }
                      });

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
      Assert.assertEquals(liveMatch, integer.intValue());
    } finally {
      db.drop();
    }
  }

  @Test
  public void testLiveProjections() throws InterruptedException {

    YTDatabaseSessionInternal db = new YTDatabaseDocumentTx("memory:OLiveQueryV2Test");
    db.activateOnCurrentThread();
    db.create();
    try {
      db.getMetadata().getSchema().createClass("test");
      db.getMetadata().getSchema().createClass("test2");
      MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

      YTLiveQueryMonitor monitor = db.live("select @class, @rid as rid, name from test", listener);
      Assert.assertNotNull(monitor);

      db.begin();
      db.command("insert into test set name = 'foo', surname = 'bar'").close();
      db.command("insert into test set name = 'foo', surname = 'baz'").close();
      db.command("insert into test2 set name = 'foo'").close();
      db.commit();

      Assert.assertTrue(listener.latch.await(5, TimeUnit.SECONDS));

      monitor.unSubscribe();

      db.begin();
      db.command("insert into test set name = 'foo', surname = 'bax'").close();
      db.command("insert into test2 set name = 'foo'").close();
      db.command("insert into test set name = 'foo', surname = 'baz'").close();
      db.commit();

      Assert.assertEquals(2, listener.ops.size());
      for (YTResult doc : listener.ops) {
        Assert.assertEquals("test", doc.getProperty("@class"));
        Assert.assertEquals("foo", doc.getProperty("name"));
        Assert.assertNull(doc.getProperty("surname"));
        YTRID rid = doc.getProperty("rid");
        Assert.assertTrue(rid.isPersistent());
      }
    } finally {
      db.drop();
    }
  }
}
