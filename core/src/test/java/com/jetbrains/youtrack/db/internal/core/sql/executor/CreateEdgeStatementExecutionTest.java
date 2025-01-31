package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CreateEdgeStatementExecutionTest extends DbTestBase {

  @Test
  public void testCreateSingleEdge() {
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
    Schema schema = db.getMetadata().getSchema();

    var vClass = "testCreateEdgeWithPropertyV";
    schema.createClass(vClass, schema.getClass("V"));

    var eClass = "testCreateEdgeWithPropertyE";
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
            "create edge "
                + eClass
                + " from "
                + v1.getIdentity()
                + " to "
                + v2.getIdentity()
                + " set name = 'theEdge'");
    db.commit();

    ExecutionPlanPrintUtils.printExecutionPlan(createREs);
    var result = db.query("select expand(outE()) from " + v1.getIdentity());
    Assert.assertNotNull(result);
    Assert.assertTrue(result.hasNext());
    var next = result.next();
    Assert.assertNotNull(next);
    Assert.assertEquals("theEdge", next.getProperty("name"));
    result.close();
  }

  @Test
  public void testCreateTwoByTwo() {
    Schema schema = db.getMetadata().getSchema();

    var vClass = "testCreateTwoByTwoV";
    schema.createClass(vClass, schema.getClass("V"));

    var eClass = "testCreateTwoByTwoE";
    schema.createClass(eClass, schema.getClass("E"));

    for (var i = 0; i < 4; i++) {
      db.begin();
      var v1 = db.newVertex(vClass);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    db.begin();
    var createREs =
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

    var result = db.query("select expand(out()) from " + vClass + " where name = 'v0'");

    Assert.assertNotNull(result);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();

    result = db.query("select expand(in()) from " + vClass + " where name = 'v2'");

    Assert.assertNotNull(result);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var next = result.next();
      Assert.assertNotNull(next);
    }
    result.close();
  }

  @Test
  public void testUpsert() {
    Schema schema = db.getMetadata().getSchema();

    var vClass1 = "testUpsertV1";
    var vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    var vClass2 = "testUpsertV2";
    var vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    var eClass = "testUpsertE";

    var eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", PropertyType.LINK, vclazz1);
    eclazz.createProperty(db, "in", PropertyType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass2);
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

    var rs = db.query("SELECT FROM " + eClass);
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
    for (var i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      var item = rs.next();
      Assert.assertEquals("bar", item.getProperty("name"));
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testUpsertHashIndex() {
    Schema schema = db.getMetadata().getSchema();

    var vClass1 = "testUpsertHashIndexV1";
    var vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    var vClass2 = "testUpsertHashIndexV2";
    var vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    var eClass = "testUpsertHashIndexE";

    var eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", PropertyType.LINK, vclazz1);
    eclazz.createProperty(db, "in", PropertyType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass2);
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

    var rs = db.query("SELECT FROM " + eClass);
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
    for (var i = 0; i < 4; i++) {
      Assert.assertTrue(rs.hasNext());
      rs.next();
    }
    Assert.assertFalse(rs.hasNext());
    rs.close();
  }

  @Test
  public void testBreakUniqueWithoutUpsert() {
    Schema schema = db.getMetadata().getSchema();

    var vClass1 = "testBreakUniqueWithoutUpsertV1";
    var vclazz1 = schema.createClass(vClass1, schema.getClass("V"));

    var vClass2 = "testBreakUniqueWithoutUpsertV2";
    var vclazz2 = schema.createClass(vClass2, schema.getClass("V"));

    var eClass = "testBreakUniqueWithoutUpsertE";

    var eclazz = schema.createClass(eClass, schema.getClass("E"));
    eclazz.createProperty(db, "out", PropertyType.LINK, vclazz1);
    eclazz.createProperty(db, "in", PropertyType.LINK, vclazz2);

    db.command("CREATE INDEX " + eClass + "out_in ON " + eclazz + " (out, in) UNIQUE");
    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass2);
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

    var rs = db.query("SELECT FROM " + eClass);
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
    } catch (RecordDuplicatedException | CommandExecutionException e) {
      // OK
    }
  }

  @Test
  public void testUpsertNoIndex() {
    Schema schema = db.getMetadata().getSchema();

    var vClass1 = "testUpsertNoIndexV1";
    schema.createClass(vClass1, schema.getClass("V"));

    var vClass2 = "testUpsertNoIndexV2";
    schema.createClass(vClass2, schema.getClass("V"));

    var eClass = "testUpsertNoIndexE";

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass1);
      v1.setProperty("name", "v" + i);
      v1.save();
      db.commit();
    }

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass2);
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
    } catch (CommandExecutionException e) {
      // OK
    }
  }

  @Test
  public void testPositionalParams() {

    var vClass1 = "testPositionalParamsV";
    db.createVertexClass(vClass1);

    var eClass = "testPositionalParamsE";
    db.createEdgeClass(eClass);

    for (var i = 0; i < 2; i++) {
      db.begin();
      var v1 = db.newVertex(vClass1);
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

    var result =
        db.query("select from " + eClass + " where out.name = 'v0' AND in.name = 'v1'");
    Assert.assertTrue(result.hasNext());
    result.close();
  }
}
