package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OptimizeDatabaseExecutionTest extends DbTestBase {

  @Test
  public void test() {
    Schema schema = session.getMetadata().getSchema();

    var vClass = "testCreateSingleEdgeV";
    schema.createClass(vClass, schema.getClass("V"));

    var eClass = "testCreateSingleEdgeE";
    schema.createClass(eClass, schema.getClass("E"));

    session.begin();
    var v1 = session.newVertex(vClass);
    v1.setProperty("name", "v1");
    v1.save();
    session.commit();

    session.begin();
    var v2 = session.newVertex(vClass);
    v2.setProperty("name", "v2");
    v2.save();
    session.commit();

    session.begin();
    var createREs =
        session.command(
            "create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity());
    session.commit();

    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    var result = session.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v2", next.getProperty("name"));
    result.close();

    session.begin();
    session.command("optimize database -LWEDGES").close();
    session.commit();

    var rs = session.query("select from E");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }
}
