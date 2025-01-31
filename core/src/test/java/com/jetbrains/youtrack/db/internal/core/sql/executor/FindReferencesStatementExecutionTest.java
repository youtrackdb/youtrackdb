package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class FindReferencesStatementExecutionTest extends DbTestBase {

  @Test
  public void testLink() {
    var name = "testLink1";
    var name2 = "testLink2";
    db.getMetadata().getSchema().createClass(name);
    db.getMetadata().getSchema().createClass(name2);

    db.begin();
    var linked = (EntityImpl) db.newEntity(name);
    linked.field("foo", "bar");
    linked.save();
    db.commit();

    Set<RID> ridsToMatch = new HashSet<>();

    for (var i = 0; i < 10; i++) {
      db.begin();
      linked = db.bindToSession(linked);
      var doc = (EntityImpl) db.newEntity(name2);
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

    var result = db.query("find references " + linked.getIdentity());

    printExecutionPlan(result);

    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      ridsToMatch.remove(next.getProperty("referredBy"));
    }

    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(ridsToMatch.isEmpty());
    result.close();
  }
}
