package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlForeach extends DBTestBase {

  @Test
  public void testForeach() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    ODocument doc = new ODocument("Test");
    db.save(doc);
    db.commit();

    OResultSet result =
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
