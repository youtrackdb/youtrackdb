package com.orientechnologies.orient.server.network;

import com.orientechnologies.common.exception.OException;
import com.orientechnologies.common.io.OFileUtils;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.OLiveQueryMonitor;
import com.orientechnologies.orient.core.db.OLiveQueryResultListener;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
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
  private OxygenDB oxygenDB;
  private ODatabaseSession db;

  @Before
  public void before() throws Exception {
    OGlobalConfiguration.SERVER_BACKWARD_COMPATIBILITY.setValue(false);
    server = new OServer(false);
    server.startup(
        getClass()
            .getClassLoader()
            .getResourceAsStream(
                "com/orientechnologies/orient/server/network/orientdb-server-config.xml"));
    server.activate();
    oxygenDB = new OxygenDB("remote:localhost:", "root", "root", OxygenDBConfig.defaultConfig());
    oxygenDB.execute(
        "create database ? memory users (admin identified by 'admin' role admin)",
        OLiveQueryRemoteTest.class.getSimpleName());
    db = oxygenDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "admin", "admin");
  }

  @After
  public void after() {
    db.close();
    oxygenDB.close();
    server.shutdown();

    Oxygen.instance().shutdown();
    OFileUtils.deleteRecursively(new File(server.getDatabaseDirectory()));
    Oxygen.instance().startup();
  }

  static class MyLiveQueryListener implements OLiveQueryResultListener {
    public CountDownLatch latch;
    public CountDownLatch ended = new CountDownLatch(1);

    public MyLiveQueryListener(CountDownLatch latch) {
      this.latch = latch;
    }

    public List<OResult> ops = new ArrayList<OResult>();

    @Override
    public void onCreate(ODatabaseSession database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onUpdate(ODatabaseSession database, OResult before, OResult after) {
      ops.add(after);
      latch.countDown();
    }

    @Override
    public void onDelete(ODatabaseSession database, OResult data) {
      ops.add(data);
      latch.countDown();
    }

    @Override
    public void onError(ODatabaseSession database, OException exception) {
    }

    @Override
    public void onEnd(ODatabaseSession database) {
      ended.countDown();
    }
  }

  @Test
  public void testRidSelect() throws InterruptedException {
    MyLiveQueryListener listener = new MyLiveQueryListener(new CountDownLatch(1));
    db.begin();
    OVertex item = db.newVertex();
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
      ORID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }

  @Test
  @Ignore
  public void testRestrictedLiveInsert() throws ExecutionException, InterruptedException {
    OSchema schema = db.getMetadata().getSchema();
    OClass oRestricted = schema.getClass("ORestricted");
    schema.createClass("test", oRestricted);

    int liveMatch = 1;
    OResultSet query = db.query("select from OUSer where name = 'reader'");

    final OIdentifiable reader = query.next().getIdentity().orElse(null);
    final OIdentifiable current = db.getUser().getIdentity(db);

    ExecutorService executorService = Executors.newSingleThreadExecutor();

    final CountDownLatch latch = new CountDownLatch(1);
    final CountDownLatch dataArrived = new CountDownLatch(1);
    Future<Integer> future =
        executorService.submit(
            new Callable<Integer>() {
              @Override
              public Integer call() throws Exception {
                ODatabaseSession db =
                    oxygenDB.open(OLiveQueryRemoteTest.class.getSimpleName(), "reader", "reader");

                final AtomicInteger integer = new AtomicInteger(0);
                db.live(
                    "live select from test",
                    new OLiveQueryResultListener() {

                      @Override
                      public void onCreate(ODatabaseSession database, OResult data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onUpdate(
                          ODatabaseSession database, OResult before, OResult after) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onDelete(ODatabaseSession database, OResult data) {
                        integer.incrementAndGet();
                        dataArrived.countDown();
                      }

                      @Override
                      public void onError(ODatabaseSession database, OException exception) {
                      }

                      @Override
                      public void onEnd(ODatabaseSession database) {
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
        new ArrayList<OIdentifiable>() {
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
      OElement elem = db.newElement("test");
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
      ORID rid = doc.getProperty("@rid");
      Assert.assertTrue(rid.isPersistent());
    }
  }
}
