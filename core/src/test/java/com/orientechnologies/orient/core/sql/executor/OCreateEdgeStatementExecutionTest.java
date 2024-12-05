package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateEdgeStatementExecutionTest extends DBTestBase {

  @Test
  public void testCreateSingleEdge() {
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

    result = db.query("select expand(in()) from " + v2.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("v1", next.getProperty("name"));
    result.close();
  }

  @Test
  public void testCreateEdgeWithProperty() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateEdgeWithPropertyV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateEdgeWithPropertyE";
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
            "create edge "
                + eClass
                + " from "
                + v1.getIdentity()
                + " to "
                + v2.getIdentity()
                + " set name = 'theEdge'");
    db.commit();

    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    YTResultSet result = db.query("select expand(outE()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    YTResult next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("theEdge", next.getProperty("name"));
    result.close();
  }

  @Test
  public void testCreateTwoByTwo() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass = "testCreateTwoByTwoV";
    schema.createClass(vClass, schema.getClass("V"));

    String eClass = "testCreateTwoByTwoE";
    schema.createClass(eClass, schema.getClass("E"));

    for (int i = 0; i < 4; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    YTResultSet createREs =
        db.command(
            "create edge "
                + eClass
                + " from (select from "
                + vClass
                + " where name in ['v0', 'v1']) to  (select from "
                + vClass
                + " where name in ['v2', 'v3'])");
    db.commit();
    ExecutionPlanPrintUtils.printExecutionPlan(createREs);

    YTResultSet result = db.query("select expand(out()) from " + vClass + " where name = 'v0'");

    Assert.assertNotNull(result);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      YTResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();

    result = db.query("select expand(in()) from " + vClass + " where name = 'v2'");

    Assert.assertNotNull(result);
    for (int i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      YTResult next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();
  }

  @Test
  public void testUpsert() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertV1";
    YTClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertV2";
    YTClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertE";

    YTClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", YTType.LINK, vclazz1);
    eclazz.createProperty(db, "in", YTType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0') SET name = 'foo'")
        .close();
    db.commit();

    YTResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " UPSERT from (select from "
                + vClass1
                + ") to  (select from "
                + vClass2
                + ") SET name = 'bar'")
        .close();
    db.commit();

    rs = db.query("SELECT FROM " + eclazz);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      YTResult item = rs.next();
      Assert.assertEquals("bar", item.getProperty("name"));
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testUpsertHashIndex() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertHashIndexV1";
    YTClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertHashIndexV2";
    YTClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertHashIndexE";

    YTClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", YTType.LINK, vclazz1);
    eclazz.createProperty(db, "in", YTType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0')")
        .close();
    db.commit();

    YTResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " UPSERT from (select from "
                + vClass1
                + ") to  (select from "
                + vClass2
                + ")")
        .close();
    db.commit();

    rs = db.query("SELECT FROM " + eclazz);
    for (int i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBreakUniqueWithoutUpsert() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testBreakUniqueWithoutUpsertV1";
    YTClass vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testBreakUniqueWithoutUpsertV2";
    YTClass vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testBreakUniqueWithoutUpsertE";

    YTClass eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", YTType.LINK, vclazz1);
    eclazz.createProperty(db, "in", YTType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " where name = 'v0') to  (select from "
                + vClass2
                + " where name = 'v0')")
        .close();
    db.commit();

    YTResultSet rs = db.query("SELECT FROM " + eClass);
    Assert.assertTrue(rs.hasNext());
    rs.next();
    Assert.assertFalse(rs.hasNext());
    rs.close();

    try {
      db.begin();
      db.command(
              "CREATE EDGE "
                  + eClass
                  + " from (select from "
                  + vClass1
                  + ") to  (select from "
                  + vClass2
                  + ")")
          .close();
      db.commit();
      Assert.fail();
    } catch (YTRecordDuplicatedException | YTCommandExecutionException e) {

    }
  }

  @Test
  public void testUpsertNoIndex() {
    YTSchema schema = db.getMetadata().getSchema();

    String vClass1 = "testUpsertNoIndexV1";
    schema.createClass(vClass1, schema.getClass("V"));

    String vClass2 = "testUpsertNoIndexV2";
    schema.createClass(vClass2, schema.getClass("V"));

    String eClass = "testUpsertNoIndexE";

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass2);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    try {
      db.command(
              "CREATE EDGE "
                  + eClass
                  + " UPSERT from (select from "
                  + vClass1
                  + ") to  (select from "
                  + vClass2
                  + ")")
          .close();
      Assert.fail();
    } catch (YTCommandExecutionException e) {

    }
  }

  @Test
  public void testPositionalParams() {

    String vClass1 = "testPositionalParamsV";
    db.createVertexClass(vClass1);

    String eClass = "testPositionalParamsE";
    db.createEdgeClass(eClass);

    for (int i = 0; i < 2; i++) {
      db.begin();
      YTVertex v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    db.command(
            "CREATE EDGE "
                + eClass
                + " from (select from "
                + vClass1
                + " WHERE name = ? ) to  (select from "
                + vClass1
                + " WHERE name = ? )",
            "v0",
            "v1")
        .close();
    db.commit();

    YTResultSet result =
        db.query("select from " + eClass + " where out.name = 'v0' AND in.name = 'v1'");
    Assert.assertTrue(result.hasNext());
    result.close();
  }
}
