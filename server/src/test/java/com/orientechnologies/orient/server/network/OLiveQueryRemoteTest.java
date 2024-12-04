package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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
  private YTDatabaseSessionInternal db;

  @Before
  public void before() throws Exception {
    YTGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
    youTrackDB = new YouTrackDB("remote:localhost:", "root", "root",
        YouTrackDBConfig.defaultConfig());
    youTrackDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OLiveQueryRemoteTest.class.getSimpleName());
    db = (YTDatabaseSessionInternal) youTrackDB.open(OLiveQueryRemoteTest.class.getSimpleName(),
        "admin", "admin");
  }

  @After
  public void after() {
    db.close();
    youTrackDB.close();
    server.shutdown();

    YouTrackDBManager.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    YouTrackDBManager.instance().startup();
  }

  static class MyLiveQueryListener implements OLiveQueryResultListener {

    public CountDownLatch latch;
    public CountDownLatch ended = new CountDownLatch(1);

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<OResult> ops = new ArrayList<OResult>();

    @Override
    public void onCreate(YTDatabaseSession database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(YTDatabaseSession database, OResult before, OResult after) {
      ops.add(after);
      latch.countDown();
    }

    @Override
    public void onDelete(YTDatabaseSession database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onError(YTDatabaseSession database, YTException exception) {
    }

    @Override
    public void onEnd(YTDatabaseSession database) {
      ended.countDown();
    }
  }

  @Test
  public void testRidSelect() throws InterruptedException {
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(1));
    db.begin();
    YTVertex item = db.newVertex();
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

    OLiveQueryMonitor monitor = db.live("select from test", listener);
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
    for (OResult doc : listener.ops) {
      Assert.assertEquals("test", doc.getProperty("@class"));
      Assert.assertEquals("foo", doc.getProperty("name"));
      YTRID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }

  @Test
  @Ignore
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    YTSchema schema = db.getMetadata().getSchema();
    YTClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 1;
    OResultSet query = db.query("select from OUSer where name = 'reader'");

    final YTIdentifiable reader = query.next().getIdentity().orElse(null);
    final YTIdentifiable current = db.getUser().getIdentity(db);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(1);
    Future<Integer> future =
        executorService.submit(
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                YTDatabaseSession db =
                    youTrackDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "reader", "reader");

                final AtomicInteger integer = new AtomicInteger(0);
                db.live(
                    "live select from test",
                    new OLiveQueryResultListener() {

                      @Override
                      public void onCreate(YTDatabaseSession database, OResult data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onUpdate(
                          YTDatabaseSession database, OResult before, OResult after) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onDelete(YTDatabaseSession database, OResult data) {
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
                Assert.assertTrue(dataArrived.await(2, TimeUnit.MINUTES));
                return integer.get();
              }
            });

    latch.await();

    query.close();
    db.command("insert into test set name = 'foo', surname = 'bar'");

    db.command(
        "insert into test set name = 'foo', surname = 'bar', _allow=?",
        new ArrayList<YTIdentifiable>() {
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

    OLiveQueryMonitor monitor = db.live("select from test", listener);
    Assert.assertNotNull(monitor);

    db.begin();
    for (int i = 0; i < txSize; i++) {
      YTEntity elem = db.newElement("test");
      elem.setProperty("name", "foo");
      elem.setProperty("surname", "bar" + i);
      elem.save();
    }
    db.commit();

    Assert.assertTrue(listener.latch.await(1, TimeUnit.MINUTES));

    Assert.assertEquals(txSize, listener.ops.size());
    for (OResult doc : listener.ops) {
      Assert.assertEquals("test", doc.getProperty("@class"));
      Assert.assertEquals("foo", doc.getProperty("name"));
      YTRID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }
}
