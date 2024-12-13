package com.orientechnologies.orient.server.network;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBEnginesManager;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.query.LiveQueryMonitor;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.orientechnologies.orient.server.OServer;
import java.io.File;
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
import org.junit.Ignore;
import org.junit.Test;

/**
 *
 */
public class OLiveQueryRemoteTest {

  private OServer server;
  private YouTrackDB youTrackDB;
  private DatabaseSessionInternal db;

  @Before
  public void before() throws Exception {
    GlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
    youTrackDB = new YouTrackDBImpl("remote:localhost:", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OLiveQueryRemoteTest.class.getSimpleName());
    db = (DatabaseSessionInternal) youTrackDB.open(OLiveQueryRemoteTest.class.getSimpleName(),
        "admin", "admin");
  }

  @After
  public void after() {
    db.close();
    youTrackDB.close();
    server.shutdown();

    YouTrackDBEnginesManager.instance().shutdown();
    FileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    YouTrackDBEnginesManager.instance().startup();
  }

  static class MyLiveQueryListener implements LiveQueryResultListener {

    public CountDownLatch latch;
    public CountDownLatch ended = new CountDownLatch(1);

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<Result> ops = new ArrayList<Result>();

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
      ended.countDown();
    }
  }

  @Test
  public void testRidSelect() throws InterruptedException {
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(1));
    db.begin();
    Vertex item = db.newVertex();
    item.save();
    db.commit();

    db.live("LIVE SELECT FROM " + item.getIdentity(), listener);
    db.begin();
    item = db.load(item.getIdentity());
    item.setProperty("x", "z");
    item.save();
    db.commit();

    Assert.assertTrue(listener.latch.await(10, TimeUnit.SECONDS));
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
    Assert.assertTrue(listener.ended.await(1, TimeUnit.MINUTES));

    db.begin();
    db.command("insert into test set name = 'foo', surname = 'bax'");
    db.command("insert into test2 set name = 'foo'");
    db.command("insert into test set name = 'foo', surname = 'baz'");
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
  @Ignore
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 1;
    ResultSet query = db.query("select from OUSer where name = 'reader'");

    final Identifiable reader = query.next().getIdentity().orElse(null);
    final Identifiable current = db.geCurrentUser().getIdentity(db);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(1);
    Future<Integer> future =
        executorService.submit(
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                DatabaseSession db =
                    youTrackDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "reader", "reader");

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
                Assert.assertTrue(dataArrived.await(2, TimeUnit.MINUTES));
                return integer.get();
              }
            });

    latch.await();

    query.close();
    db.command("insert into test set name = 'foo', surname = 'bar'");

    db.command(
        "insert into test set name = 'foo', surname = 'bar', _allow=?",
        new ArrayList<Identifiable>() {
          {
            add(current);
            add(reader);
          }
        });

    Integer integer = future.get();
    Assert.assertEquals(liveMatch, integer.intValue());
  }

  @Test
  public void testBatchWithTx() throws InterruptedException {

    db.getMetadata().getSchema().createClass("test");
    db.getMetadata().getSchema().createClass("test2");

    int txSize = 100;

    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(txSize));

    LiveQueryMonitor monitor = db.live("select from test", listener);
    Assert.assertNotNull(monitor);

    db.begin();
    for (int i = 0; i < txSize; i++) {
      Entity elem = db.newEntity("test");
      elem.setProperty("name", "foo");
      elem.setProperty("surname", "bar" + i);
      elem.save();
    }
    db.commit();

    Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

    Assert.assertEquals(txSize, listener.ops.size());
    for (Result doc : listener.ops) {
      Assert.assertEquals("test", doc.getProperty("@class"));
      Assert.assertEquals("foo", doc.getProperty("name"));
      RID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }
}
