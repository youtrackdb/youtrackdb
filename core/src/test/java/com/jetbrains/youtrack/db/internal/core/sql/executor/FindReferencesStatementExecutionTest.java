package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
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
    session.getMetadata().getSchema().createClass(name);
    session.getMetadata().getSchema().createClass(name2);

    session.begin();
    var linked = (EntityImpl) session.newEntity(name);
    linked.field("foo", "bar");

    session.commit();

    Set<RID> ridsToMatch = new HashSet<>();

    for (var i = 0; i < 10; i++) {
      session.begin();
      linked = session.bindToSession(linked);
      var doc = (EntityImpl) session.newEntity(name2);
      doc.field("counter", i);
      if (i % 2 == 0) {
        doc.field("link", linked);
      }

      session.commit();
      if (i % 2 == 0) {
        ridsToMatch.add(doc.getIdentity());
      }
    }

    var result = session.query("find references " + linked.getIdentity());

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
