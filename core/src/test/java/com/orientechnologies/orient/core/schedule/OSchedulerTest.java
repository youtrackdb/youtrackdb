package com.orientechnologies.orient.core.schedule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.concur.ONeedRetryException;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabasePool;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseThreadLocalFactory;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.db.YouTrackDBConfig;
import com.orientechnologies.orient.core.metadata.function.OFunction;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests cases for the Scheduler component.
 */
public class OSchedulerTest {

  @Test
  public void scheduleSQLFunction() throws Exception {
    try (YouTrackDB context = createContext()) {
      var db =
          (YTDatabaseSessionInternal) context.cachedPool("test", "admin",
              OCreateDatabaseUtil.NEW_ADMIN_PASSWORD).acquire();
      createLogEvent(db);

      DBTestBase.assertWithTimeout(
          db,
          () -> {
            Long count = getLogCounter(db);
            Assert.assertTrue(count >= 2 && count <= 3);
          });
    }
  }

  @Test
  public void scheduleWithDbClosed() throws Exception {
    YouTrackDB context = createContext();
    {
      var db = (YTDatabaseSessionInternal) context.open("test", "admin",
          OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
      createLogEvent(db);
      db.close();
    }

    var db = context.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    DBTestBase.assertWithTimeout(
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
    try (YouTrackDB context = createContext()) {
      var db =
          (YTDatabaseSessionInternal) context.cachedPool("test", "admin",
              OCreateDatabaseUtil.NEW_ADMIN_PASSWORD).acquire();
      createLogEvent(db);

      Thread.sleep(2000);

      db.getMetadata().getScheduler().removeEvent(db, "test");

      assertThat(db.getMetadata().getScheduler().getEvents()).isEmpty();

      assertThat(db.getMetadata().getScheduler().getEvent("test")).isNull();

      // remove again
      db.getMetadata().getScheduler().removeEvent(db, "test");

      Thread.sleep(3000);

      Long count = getLogCounter(db);

      Assert.assertTrue(count >= 1 && count <= 3);
    }
  }

  @Test
  public void eventSavedAndLoaded() throws Exception {
    YouTrackDB context = createContext();
    var db =
        (YTDatabaseSessionInternal) context.open("test", "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    createLogEvent(db);
    db.close();

    Thread.sleep(1000);

    final YTDatabaseSession db2 =
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
    final YouTrackDB youTrackDb =
        new YouTrackDB(
            DBTestBase.embeddedDBUrl(getClass()),
            YouTrackDBConfig.builder()
                .addConfig(YTGlobalConfiguration.DB_POOL_MAX, 1)
                .addConfig(YTGlobalConfiguration.CREATE_DEFAULT_USERS, false)
                .build());
    if (!youTrackDb.exists("test")) {
      youTrackDb.execute(
          "create database "
              + "test"
              + " "
              + "memory"
              + " users ( admin identified by '"
              + OCreateDatabaseUtil.NEW_ADMIN_PASSWORD
              + "' role admin)");
    }
    final ODatabasePool pool =
        youTrackDb.cachedPool("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);
    var db = (YTDatabaseSessionInternal) pool.acquire();

    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());
    createLogEvent(db);
    assertEquals(db, ODatabaseRecordThreadLocal.instance().getIfDefined());

    youTrackDb.close();
  }

  @Test
  public void eventBySQL() throws Exception {
    YouTrackDB context = createContext();
    try (context;
        var db =
            (YTDatabaseSessionInternal) context.open("test", "admin",
                OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      OFunction func = createFunction(db);
      db.begin();
      db.command(
              "insert into oschedule set name = 'test',"
                  + " function = ?, rule = \"0/1 * * * * ?\", arguments = {\"note\": \"test\"}",
              func.getId(db))
          .close();
      db.commit();

      DBTestBase.assertWithTimeout(
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
                  "update oschedule set rule = \"0/2 * * * * ?\", function = ? where name = 'test'",
                  func.getId(db))
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

      DBTestBase.assertWithTimeout(
          db,
          () -> {
            long newCount = getLogCounter(db);
            Assert.assertTrue(newCount - count > 1);
          });

      long newCount = getLogCounter(db);
      Assert.assertTrue(newCount - count > 1);

      retryCount = 10;
      while (true) {
        try {
          // DELETE
          db.begin();
          db.command("delete from oschedule where name = 'test'", func.getId(db)).close();
          db.commit();
          break;
        } catch (ONeedRetryException e) {
          retryCount--;
          //noinspection BusyWait
          Thread.sleep(10);
        }
        Assert.assertTrue(retryCount >= 0);
      }

      DBTestBase.assertWithTimeout(
          db,
          () -> {
            var counter = getLogCounter(db);
            Assert.assertTrue(counter - newCount <= 1);
          });
    }
  }

  private YouTrackDB createContext() {
    final YouTrackDB youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    YouTrackDBManager.instance()
        .registerThreadDatabaseFactory(
            new TestScheduleDatabaseFactory(
                youTrackDB, "test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD));
    return youTrackDB;
  }

  private void createLogEvent(YTDatabaseSessionInternal db) {
    OFunction func = createFunction(db);

    db.executeInTx(() -> {
      Map<Object, Object> args = new HashMap<>();
      args.put("note", "test");

      new OScheduledEventBuilder()
          .setName(db, "test")
          .setRule(db, "0/1 * * * * ?")
          .setFunction(db, func)
          .setArguments(db, args)
          .build(db);
    });
  }

  private OFunction createFunction(YTDatabaseSessionInternal db) {
    db.getMetadata().getSchema().createClass("scheduler_log");

    return db.computeInTx(
        () -> {
          OFunction func = db.getMetadata().getFunctionLibrary().createFunction("logEvent");
          func.setLanguage(db, "SQL");
          func.setCode(db, "insert into scheduler_log set timestamp = sysdate(), note = :note");
          final List<String> pars = new ArrayList<>();
          pars.add("note");
          func.setParameters(db, pars);
          func.save(db);
          return func;
        });
  }

  private Long getLogCounter(final YTDatabaseSession db) {
    OResultSet resultSet =
        db.query("select count(*) as count from scheduler_log where note = 'test'");
    OResult result = resultSet.stream().findFirst().orElseThrow();
    var count = result.<Long>getProperty("count");
    resultSet.close();
    return count;
  }

  private static class TestScheduleDatabaseFactory implements ODatabaseThreadLocalFactory {

    private final YouTrackDB context;
    private final String database;
    private final String username;
    private final String password;

    public TestScheduleDatabaseFactory(
        YouTrackDB context, String database, String username, String password) {
      this.context = context;
      this.database = database;
      this.username = username;
      this.password = password;
    }

    @Override
    public YTDatabaseSessionInternal getThreadDatabase() {
      return (YTDatabaseSessionInternal) context.cachedPool(database, username, password).acquire();
    }
  }
}
