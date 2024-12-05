package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlForeach extends DBTestBase {

  @Test
  public void testForeach() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    db.save(doc);
    db.commit();

    YTResultSet result =
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
