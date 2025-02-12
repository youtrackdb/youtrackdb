package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SelectStatementExecutionTestIT extends DbTestBase {

  @Test
  public void stressTestNew() {
    var className = "stressTestNew";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 1000000; i++) {
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (var run = 0; run < 5; run++) {
      var begin = System.nanoTime();
      var result = session.query("select name from " + className + " where name <> 'name1' ");
      for (var i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        var item = result.next();
        //        Assert.assertNotNull(item);
        var name = item.getProperty("name");
        Assert.assertNotEquals("name1", name);
      }
      Assert.assertFalse(result.hasNext());
      result.close();
      var end = System.nanoTime();
      System.out.println("new: " + ((end - begin) / 1000000));
    }
  }

  public void stressTestOld() {
    var className = "stressTestOld";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 1000000; i++) {
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    for (var run = 0; run < 5; run++) {
      var begin = System.nanoTime();
      List<EntityImpl> r =
          session.query(
              new SQLSynchQuery<EntityImpl>(
                  "select name from " + className + " where name <> 'name1' "));
      //      Iterator<EntityImpl> result = r.iterator();
      for (var i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        //        EntityImpl item = result.next();
        var item = r.get(i);

        //        Assert.assertNotNull(item);
        var name = item.getProperty("name");
        Assert.assertNotEquals("name1", name);
      }
      //      Assert.assertFalse(result.hasNext());
      var end = System.nanoTime();
      System.out.println("old: " + ((end - begin) / 1000000));
    }
  }
}
