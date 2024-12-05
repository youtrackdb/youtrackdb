package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.OCreateDatabaseUtil;
import com.jetbrains.youtrack.db.internal.core.db.ODatabasePool;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.exception.YTConcurrentModificationException;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
        OCreateDatabaseUtil.createDatabase(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
  }

  @Test
  public void test() {
    YTDatabaseSession session =
        youTrackDB.open(
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    session.command("ALTER DATABASE CUSTOM useLightweightEdges = true");

    session.begin();
    session.command("create vertex v set name='a' ");
    session.command("create vertex v set name='b' ");
    session.command(
        "create edge e from (select from v where name='a') to (select from v where name='a') ");
    session.commit();

    try (YTResultSet res = session.query("select expand(out()) from v where name='a' ")) {
      assertEquals(res.stream().count(), 1);
    }
    session.close();
  }

  @Test
  public void mtTest() throws InterruptedException {

    ODatabasePool pool =
        new ODatabasePool(
            youTrackDB,
            CreateLightWeightEdgesSQLTest.class.getSimpleName(),
            "admin",
            OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    YTDatabaseSession session = pool.acquire();

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
                    try (YTDatabaseSession session1 = pool.acquire()) {
                      for (int j = 0; j < 100; j++) {

                        try {
                          session1.begin();
                          session1.command(
                              "create edge e from (select from v where id=1) to (select from v"
                                  + " where id=2) ");
                          session1.commit();
                        } catch (YTConcurrentModificationException e) {
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
    try (YTResultSet res = session.query("select sum(out().size()) as size from V where id = 1");
        YTResultSet res1 = session.query("select sum(in().size()) as size from V where id = 2")) {

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
