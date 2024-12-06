package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.CreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.DatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class CreateLightWeightEdgesSQLTest {

  private YouTrackDB youTrackDB;

  @Before
  public void before() {
    youTrackDB =
        CreateDatabaseUtil.createDatabase(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            DbTestBase.embeddedDBUrl(getClass()),
            CreateDatabaseUtil.TYPE_MEMORY);
  }

  @Test
  public void test() {
    DatabaseSession session =
        youTrackDB.open(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");

    session.begin();
    session.command("create vertex v set name='a' ");
    session.command("create vertex v set name='b' ");
    session.command(
        "create edge e from (select from v where name='a') to (select from v where name='a') ");
    session.commit();

    try (ResultSet res = session.query("select expand(out()) from v where name='a' ")) {
      assertEquals(res.stream().count(), 1);
    }
    session.close();
  }

  @Test
  public void mtTest() throws InterruptedException {

    DatabasePool pool =
        new DatabasePool(
            youTrackDB,
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            CreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    DatabaseSession session = pool.acquire();

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");

    session.begin();
    session.command("create vertex v set id = 1 ");
    session.command("create vertex v set id = 2 ");
    session.commit();

    session.close();

    CountDownLatch latch = new CountDownLatch(10);

    IntStream.range(0, 10)
        .forEach(
            (i) -> {
              new Thread(
                  () -> {
                    try (DatabaseSession session1 = pool.acquire()) {
                      for (int j = 0; j < 100; j++) {

                        try {
                          session1.begin();
                          session1.command(
                              "create edge e from (select from v where id=1) to (select from v"
                                  + " where id=2) ");
                          session1.commit();
                        } catch (ConcurrentModificationException e) {
                          // ignore
                        }
                      }
                    } finally {
                      latch.countDown();
                    }
                  })
                  .start();
            });

    latch.await();

    session = pool.acquire();
    try (ResultSet res = session.query("select sum(out().size()) as size from V where id = 1");
        ResultSet res1 = session.query("select sum(in().size()) as size from V where id = 2")) {

      Integer s1 = res.stream().findFirst().get().getProperty("size");
      Integer s2 = res1.stream().findFirst().get().getProperty("size");
      assertEquals(s1, s2);

    } finally {
      session.close();
      pool.close();
    }
  }

  @After
  public void after() {
    youTrackDB.close();
  }
}
