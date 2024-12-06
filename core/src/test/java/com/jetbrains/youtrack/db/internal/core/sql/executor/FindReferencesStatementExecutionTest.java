package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RID;
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
    String name = "testLink1";
    String name2 = "testLink2";
    db.getMetadata().getSchema().createClass(name);
    db.getMetadata().getSchema().createClass(name2);

    db.begin();
    EntityImpl linked = new EntityImpl(name);
    linked.field("foo", "bar");
    linked.save();
    db.commit();

    Set<RID> ridsToMatch = new HashSet<>();

    for (int i = 0; i < 10; i++) {
      db.begin();
      linked = db.bindToSession(linked);
      EntityImpl doc = new EntityImpl(name2);
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

    ResultSet result = db.query("find references " + linked.getIdentity());

    printExecutionPlan(result);

    for (int i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      Result next = result.next();
      ridsToMatch.remove(next.getProperty("referredBy"));
    }

    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(ridsToMatch.isEmpty());
    result.close();
  }
}
