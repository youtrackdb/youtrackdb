package com.orientechnologies.orient.server.query;

import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RemoteGraphTXTest extends BaseServerMemoryDatabase {

  public void beforeTest() {
    super.beforeTest();
    db.createClassIfNotExist("FirstV", "V");
    db.createClassIfNotExist("SecondV", "V");
    db.createClassIfNotExist("TestEdge", "E");
  }

  @Test
  public void itShouldDeleteEdgesInTx() {
    db.begin();
    db.command("create vertex FirstV set id = '1'").close();
    db.command("create vertex SecondV set id = '2'").close();
    db.commit();

    db.begin();
    try (YTResultSet resultSet =
        db.command(
            "create edge TestEdge  from ( select from FirstV where id = '1') to ( select from"
                + " SecondV where id = '2')")) {
      YTResult result = resultSet.stream().iterator().next();

      Assert.assertTrue(result.isEdge());
    }
    db.commit();

    db.begin();
    db
        .command(
            "delete edge TestEdge from (select from FirstV where id = :param1) to (select from"
                + " SecondV where id = :param2)",
            new HashMap() {
              {
                put("param1", "1");
                put("param2", "2");
              }
            })
        .stream()
        .collect(Collectors.toList());
    db.commit();

    db.begin();
    Assert.assertEquals(0, db.query("select from TestEdge").stream().count());
    List<YTResult> results =
        db.query("select bothE().size() as count from V").stream().collect(Collectors.toList());

    for (YTResult result : results) {
      Assert.assertEquals(0, (int) result.getProperty("count"));
    }
    db.commit();
  }
}
