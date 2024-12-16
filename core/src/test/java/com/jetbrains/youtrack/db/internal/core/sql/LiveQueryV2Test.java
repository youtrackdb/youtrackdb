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

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import java.util.ArrayList;
import java.util.List;
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
public class LiveQueryV2Test extends DbTestBase {

  static class MyLiveQueryListener implements LiveQueryResultListener {

    public CountDownLatch latch;

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<Result> ops = new ArrayList<>();

    @Override
    public void onCreate(DatabaseSession database, Result data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(DatabaseSession database, Result before, Result after) {
      ops.add(after);
      latch.countDown();
    }

    @Override
    public void onDelete(DatabaseSession database, Result data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onError(DatabaseSession database, BaseException exception) {
    }

    @Override
    public void onEnd(DatabaseSession database) {
    }
  }

  @Test
  public void testLiveInsert() throws InterruptedException {
    db.getMetadata().getSchema().createClass("test");
    db.getMetadata().getSchema().createClass("test2");
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

    LiveQueryMonitor monitor = db.live("select from test", listener);
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
    for (Result doc : listener.ops) {
      Assert.assertEquals("test", doc.getProperty("@class"));
      Assert.assertEquals("foo", doc.getProperty("name"));
      RID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }

  @Test
  public void testLiveInsertOnCluster() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("test");

    int defaultCluster = clazz.getClusterIds()[0];
    String clusterName = db.getStorage().getClusterNameById(defaultCluster);

    LiveQueryV2Test.MyLiveQueryListener listener =
        new MyLiveQueryListener(new CountDownLatch(1));

    db.live(" select from cluster:" + clusterName, listener);

    db.begin();
    db.command("insert into cluster:" + clusterName + " set name = 'foo', surname = 'bar'");
    db.commit();

    try {
      Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Assert.assertEquals(1, listener.ops.size());
    for (Result doc : listener.ops) {
      Assert.assertEquals("foo", doc.getProperty("name"));
      RID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
      Assert.assertNotNull(rid);
    }
  }

  @Test
  public void testLiveWithWhereCondition() {
    db.getMetadata().getSchema().createClass("test");

    LiveQueryV2Test.MyLiveQueryListener listener =
        new MyLiveQueryListener(new CountDownLatch(1));

    db.live("select from V where id = 1", listener);

    db.begin();
    db.command("insert into V set id = 1");
    db.commit();

    try {
      Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
    Assert.assertEquals(1, listener.ops.size());
    for (Result doc : listener.ops) {
      Assert.assertEquals(doc.getProperty("id"), Integer.valueOf(1));
      RID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
      Assert.assertNotNull(rid);
    }
  }

  @Test
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 2;
    List<Entity> query =
        db.query("select from OUSer where name = 'reader'").toEntityList();

    final Identifiable reader = query.getFirst().getIdentity();
    final Identifiable current = db.geCurrentUser().getIdentity(db);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(liveMatch);
    Future<Integer> future =
        executorService.submit(
            () -> {
              try (var db = openDatabase(readerUser, readerPassword)) {
                final AtomicInteger integer = new AtomicInteger(0);
                db.live(
                    "live select from test",
                    new LiveQueryResultListener() {

                      @Override
                      public void onCreate(DatabaseSession database, Result data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onUpdate(
                          DatabaseSession database, Result before, Result after) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onDelete(DatabaseSession database, Result data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onError(DatabaseSession database, BaseException exception) {
                      }

                      @Override
                      public void onEnd(DatabaseSession database) {
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
            new ArrayList<Identifiable>() {
              {
                add(current);
                add(reader);
              }
            })
        .close();
    db.commit();

    Integer integer = future.get();
    Assert.assertEquals(liveMatch, integer.intValue());
  }

  @Test
  public void testLiveProjections() throws InterruptedException {
    db.getMetadata().getSchema().createClass("test");
    db.getMetadata().getSchema().createClass("test2");
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(2));

    LiveQueryMonitor monitor = db.live("select @class, @rid as rid, name from test", listener);
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
    for (Result doc : listener.ops) {
      Assert.assertEquals("test", doc.getProperty("@class"));
      Assert.assertEquals("foo", doc.getProperty("name"));
      Assert.assertNull(doc.getProperty("surname"));
      RID rid = doc.getProperty("rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }
}
