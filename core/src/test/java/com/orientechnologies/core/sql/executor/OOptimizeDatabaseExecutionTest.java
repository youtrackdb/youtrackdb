package com.orientechnologies.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.record.YTVertex;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OOptimizeDatabaseExecutionTest extends DBTestBase {

  @Test
  public void test() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateSingleEdgeV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateSingleEdgeE";
    schema.createClass(eClass, schema.getClass("E"));

    db.begin();
    YTVertex v1 = db.newVertex(vClass);
    v1.setProperty("name", "v1");
    v1.save();
    db.commit();

    db.begin();
    YTVertex v2 = db.newVertex(vClass);
    v2.setProperty("name", "v2");
    v2.save();
    db.commit();

    db.begin();
    YTResultSet createREs =
        db.command(
            "create edge " + eClass + " from " + v1.getIdentity() + " to " + v2.getIdentity());
    db.commit();

    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    YTResultSet result = db.query("select expand(out()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v2", next.getProperty("name"));
    result.close();

    db.begin();
    db.command("optimize database -LWEDGES").close();
    db.commit();

    YTResultSet rs = db.query("select from E");
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }
}
