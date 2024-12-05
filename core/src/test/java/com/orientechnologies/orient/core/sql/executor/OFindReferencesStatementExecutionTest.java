package com.orientechnologies.orient.core.sql.executor;

import static com.orientechnologies.orient.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OFindReferencesStatementExecutionTest extends DBTestBase {

  @Test
  public void testLink() {
    String name = "testLink1";
    String name2 = "testLink2";
    db.getMetadata().getSchema().createClass(name);
    db.getMetadata().getSchema().createClass(name2);

    db.begin();
    YTEntityImpl linked = new YTEntityImpl(name);
    linked.field("foo", "bar");
    linked.save();
    db.commit();

    Set<YTRID> ridsToMatch = new HashSet<>();

    for (int i = 0; i < 10; i++) {
      db.begin();
      linked = db.bindToSession(linked);
      YTEntityImpl doc = new YTEntityImpl(name2);
      doc.field("counter", i);
      if (i % 2 == 0) {
        doc.field("link", linked);
      }
      doc.save();
      db.commit();
      if (i % 2 == 0) {
        ridsToMatch.add(doc.getIdentity());
      }
    }

    YTResultSet result = db.query("find references " + linked.getIdentity());

    printExecutionPlan(result);

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      YTResult next = result.next();
      ridsToMatch.remove(next.getProperty("referredBy"));
    }

    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(ridsToMatch.isEmpty());
    result.close();
  }
}
