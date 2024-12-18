package com.jetbrains.youtrack.db.internal.server.query;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.query.LiveQueryResultListener;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.server.BaseServerMemoryDatabase;
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
    try (ResultSet resultSet =
        db.command("create edge TestEdge  from (select from FirstV) to (select from SecondV)")) {
      Result result = resultSet.stream().iterator().next();

      Assert.assertTrue(result.isEdge());
    }
    db.commit();

    AtomicLong l = new AtomicLong(0);

    db.live(
        "select from SecondV",
        new LiveQueryResultListener() {

          @Override
          public void onUpdate(DatabaseSessionInternal database, Result before, Result after) {
            l.incrementAndGet();
          }

          @Override
          public void onError(DatabaseSession database, BaseException exception) {
          }

          @Override
          public void onEnd(DatabaseSession database) {
          }

          @Override
          public void onDelete(DatabaseSessionInternal database, Result data) {
          }

          @Override
          public void onCreate(DatabaseSessionInternal database, Result data) {
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
