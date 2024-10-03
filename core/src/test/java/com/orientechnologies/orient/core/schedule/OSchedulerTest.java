package com.orientechnologies.orient.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.Orient;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.db.*;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests cases for the Scheduler component.
 *
 * @author Enrico Risa
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {
    try (OrientDB context = createContext()) {
      final ODatabaseSession db =
          context.cachedPool("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD).acquire();
      createLogEvent(db);

      BaseMemoryDatabase.assertWithTimeout(
          db,
          () -> {
            Long count = getLogCounter(db);
            Assert.assertTrue(count >= 2 && count <= 3);
          });
    }
  }

  @Test
  public void scheduleWithDbClosed() throws Exception {
    OrientDB context = createContext();
    {
      ODatabaseSession db = context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      createLogEvent(db);
      db.close();
    }

    var db = context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    BaseMemoryDatabase.assertWithTimeout(
        db,
        () -> {
          Long count = getLogCounter(db);
          Assert.assertTrue(count >= 2);
        });

    db.close();
    context.close();
  }

  @Test
  public void eventLifecycle() throws Exception {
    try (OrientDB context = createContext()) {
      ODatabaseSession db =
          context.cachedPool("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD).acquire();
      createLogEvent(db);

      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent("test");

      assertThat(db.getMetadata().getScheduler().getEvents()).isEmpty();

      assertThat(db.getMetadata().getScheduler().getEvent("test")).isNull();

      // remove again
      db.getMetadata().getScheduler().removeEvent("test");

      Thread.sleep(3000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 1 && count <= 3);
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {
    OrientDB context = createContext();
    final ODatabaseSession db =
        context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    createLogEvent(db);
    db.close();

    Thread.sleep(1000);

    final ODatabaseSession db2 =
        context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    try {
      Thread.sleep(4000);
      Long count = getLogCounter(db2);
      Assert.assertTrue(count >= 2);

    } finally {
      db2.close();
      context.close();
    }
  }

  @Test
  public void testScheduleEventWithMultipleActiveDatabaseConnections() {
    final OrientDB orientDb =
        new OrientDB(
            "embedded:",
            OrientDBConfig.builder()
                .addConfig(OGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(OGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!orientDb.exists("test")) {
      orientDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    final ODatabasePool pool =
        orientDb.cachedPool("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    final ODatabaseSession db = pool.acquire();

    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());
    createLogEvent(db);
    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());

    orientDb.close();
  }

  @Test
  public void eventBySQL() throws Exception {
    OrientDB context = createContext();
    try (context;
        ODatabaseSession db =
            context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      OFunction func = createFunction(db);
      db.begin();
      db.command(
              "insert into oschedule set name = 'test', function = ?, rule = \"0/1 * * * * ?\"",
              func.getId())
          .close();
      db.commit();

      BaseMemoryDatabase.assertWithTimeout(
          db,
          () -> {
            long count = getLogCounter(db);
            Assert.assertTrue(count >= 2);
          });

      final long count = getLogCounter(db);
      Assert.assertTrue(count >= 2);

      int retryCount = 10;
      while (true) {
        try {
          db.begin();
          db.command(
                  "update oschedule set rule = \"0/2 * * * * ?\" where name = 'test'", func.getId())
              .close();
          db.commit();
          break;
        } catch (ONeedRetryException e) {
          retryCount--;
          //noinspection BusyWait
          Thread.sleep(10);
        }
        Assert.assertTrue(retryCount >= 0);
      }

      BaseMemoryDatabase.assertWithTimeout(
          db,
          () -> {
            long newCount = getLogCounter(db);
            Assert.assertTrue(newCount - count > 1);
          });

      long newCount = getLogCounter(db);
      Assert.assertTrue(newCount - count > 1);

      // DELETE
      db.command("delete from oschedule where name = 'test'", func.getId()).close();

      BaseMemoryDatabase.assertWithTimeout(
          db,
          () -> {
            var counter = getLogCounter(db);
            Assert.assertTrue(counter - newCount <= 1);
          });
    }
  }

  private OrientDB createContext() {
    final OrientDB orientDB =
        OCreateDatabaseUtil.createDatabase("test", "embedded:", OCreateDatabaseUtil.TYPE_MEMORY);
    Orient.instance()
        .registerThreadDatabaseFactory(
            new TestScheduleDatabaseFactory(
                orientDB, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD));
    return orientDB;
  }

  private void createLogEvent(ODatabaseSession db) {
    OFunction func = createFunction(db);

    Map<Object, Object> args = new HashMap<>();
    args.put("note", "test");
    db.getMetadata()
        .getScheduler()
        .scheduleEvent(
            new OScheduledEventBuilder()
                .setName("test")
                .setRule("0/1 * * * * ?")
                .setFunction(func)
                .setArguments(args)
                .build());
  }

  private OFunction createFunction(ODatabaseSession db) {
    db.getMetadata().getSchema().createClass("scheduler_log");

    return db.computeInTx(
        () -> {
          OFunction func = db.getMetadata().getFunctionLibrary().createFunction("logEvent");
          func.setLanguage("SQL");
          func.setCode("insert into scheduler_log set timestamp = sysdate(), note = :note");
          final List<String> pars = new ArrayList<>();
          pars.add("note");
          func.setParameters(pars);
          func.save();
          return func;
        });
  }

  private Long getLogCounter(final ODatabaseSession db) {
    OResult result =
        db.query("select count(*) as count from scheduler_log").stream().findFirst().get();
    return result.getProperty("count");
  }

  private static class TestScheduleDatabaseFactory implements ODatabaseThreadLocalFactory {

    private final OrientDB context;
    private final String database;
    private final String username;
    private final String password;

    public TestScheduleDatabaseFactory(
        OrientDB context, String database, String username, String password) {
      this.context = context;
      this.database = database;
      this.username = username;
      this.password = password;
    }

    @Override
    public ODatabaseDocumentInternal getThreadDatabase() {
      return (ODatabaseDocumentInternal) context.cachedPool(database, username, password).acquire();
    }
  }
}
