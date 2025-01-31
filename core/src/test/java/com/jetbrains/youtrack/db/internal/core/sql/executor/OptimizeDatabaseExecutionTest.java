package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.record.Vertex;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OptimizeDatabaseExecutionTest extends DbTestBase {

  @Test
  public void test() {
    Schema schema = db.getMetadata().getSchema();

    var vClass = "testCreateSingleEdgeV";
    schema.createClass(vClass, schema.getClass("V"));

    var eClass = "testCreateSingleEdgeE";
    schema.createClass(eClass, schema.getClass("E"));

    db.begin();
    var v1 = db.newVertex(vClass);
    v1.setProperty("name", "v1");
    v1.save();
    db.commit();

    db.begin();
    var v2 = db.newVertex(vClass);
    v2.setProperty("name", "v2");
    v2.save();
    db.commit();

    db.begin();
    var createREs =
        db.command(
            "create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity());
    db.commit();

    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    var result = db.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v2", next.getProperty("name"));
    result.close();

    db.begin();
    db.command("optimize database -LWEDGES").close();
    db.commit();

    var rs = db.query("select from E");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }
}
