package com.orientechnologies.orient.server.query;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSession;
import com.jetbrains.youtrack.db.internal.core.db.YTLiveQueryResultListener;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.Assert;
import org.junit.Test;

public class RemoteGraphLiveQueryTest extends BaseServerMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    db.createClassIfNotExist("FirstV", "V");
    db.createClassIfNotExist("SecondV", "V");
    db.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void testLiveQuery() throws InterruptedException {

    db.begin();
    db.command("create vertex FirstV set id = '1'").close();
    db.command("create vertex SecondV set id = '2'").close();
    db.commit();

    db.begin();
    try (YTResultSet resultSet =
        db.command("create edge TestEdge  from (select from FirstV) to (select from SecondV)")) {
      YTResult result = resultSet.stream().iterator().next();

      Assert.assertTrue(result.isEdge());
    }
    db.commit();

    AtomicLong l = new AtomicLong(0);

    db.live(
        "select from SecondV",
        new YTLiveQueryResultListener() {

          @Override
          public void onUpdate(YTDatabaseSession database, YTResult before, YTResult after) {
            l.incrementAndGet();
          }

          @Override
          public void onError(YTDatabaseSession database, YTException exception) {
          }

          @Override
          public void onEnd(YTDatabaseSession database) {
          }

          @Override
          public void onDelete(YTDatabaseSession database, YTResult data) {
          }

          @Override
          public void onCreate(YTDatabaseSession database, YTResult data) {
          }
        },
        new HashMap<String, String>());

    db.begin();
    db.command("update SecondV set id = 3");
    db.commit();

    Thread.sleep(100);

    Assert.assertEquals(1L, l.get());
  }
}
