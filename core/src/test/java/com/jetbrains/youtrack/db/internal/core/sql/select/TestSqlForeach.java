package com.jetbrains.youtrack.db.internal.core.sql.select;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlForeach extends DbTestBase {

  @Test
  public void testForeach() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    var doc = ((EntityImpl) db.newEntity("Test"));
    db.save(doc);
    db.commit();

    var result =
        db.execute(
            "sql",
            "let $res = select from Test; foreach ($r in $res) { begin; update $r set timestamp ="
                + " sysdate(); commit;}; return $res; ");

    Assert.assertTrue(result.hasNext());

    while (result.hasNext()) {
      result.next();
    }
  }
}
