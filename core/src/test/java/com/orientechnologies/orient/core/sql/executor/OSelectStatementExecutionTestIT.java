package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OSelectStatementExecutionTestIT extends DBTestBase {

  @Test
  public void stressTestNew() {
    String className = "stressTestNew";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 1000000; i++) {
      YTEntityImpl doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }

    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      YTResultSet result = db.query("select name from " + className + " where name <> 'name1' ");
      for (int i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        YTResult item = result.next();
        //        Assert.assertNotNull(item);
        Object name = item.getProperty("name");
        Assert.assertNotEquals("name1", name);
      }
      Assert.assertFalse(result.hasNext());
      result.close();
      long end = System.nanoTime();
      System.out.println("new: " + ((end - begin) / 1000000));
    }
  }

  public void stressTestOld() {
    String className = "stressTestOld";
    db.getMetadata().getSchema().createClass(className);
    for (int i = 0; i < 1000000; i++) {
      YTEntityImpl doc = db.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
    }
    for (int run = 0; run < 5; run++) {
      long begin = System.nanoTime();
      List<YTEntityImpl> r =
          db.query(
              new OSQLSynchQuery<YTEntityImpl>(
                  "select name from " + className + " where name <> 'name1' "));
      //      Iterator<YTEntityImpl> result = r.iterator();
      for (int i = 0; i < 999999; i++) {
        //        Assert.assertTrue(result.hasNext());
        //        YTEntityImpl item = result.next();
        YTEntityImpl item = r.get(i);

        //        Assert.assertNotNull(item);
        Object name = item.getProperty("name");
        Assert.assertNotEquals("name1", name);
      }
      //      Assert.assertFalse(result.hasNext());
      long end = System.nanoTime();
      System.out.println("old: " + ((end - begin) / 1000000));
    }
  }
}
