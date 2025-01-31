package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class InsertStatementExecutionTest extends DbTestBase {

  @Test
  public void testInsertSet() {
    var className = "testInsertSet";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result = db.command("insert into " + className + " set name = 'name1'");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue() {
    var className = "testInsertValue";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command("insert into " + className + "  (name, surname) values ('name1', 'surname1')");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertValue2() {
    var className = "testInsertValue2";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command(
            "insert into "
                + className
                + "  (name, surname) values ('name1', 'surname1'), ('name2', 'surname2')");
    db.commit();

    printExecutionPlan(result);

    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name" + (i + 1), item.getProperty("name"));
      Assert.assertEquals("surname" + (i + 1), item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    names.add("name1");
    names.add("name2");
    result = db.query("select from " + className);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect1() {
    var className1 = "testInsertFromSelect1";
    db.getMetadata().getSchema().createClass(className1);

    var className2 = "testInsertFromSelect1_1";
    db.getMetadata().getSchema().createClass(className2);
    for (var i = 0; i < 10; i++) {
      db.begin();
      EntityImpl doc = db.newInstance(className1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
      db.commit();
    }

    db.begin();
    var result = db.command(
        "insert into " + className2 + " from select from " + className1);
    db.commit();

    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (var i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = db.query("select from " + className2);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testInsertFromSelect2() {
    var className1 = "testInsertFromSelect2";
    db.getMetadata().getSchema().createClass(className1);

    var className2 = "testInsertFromSelect2_1";
    db.getMetadata().getSchema().createClass(className2);
    for (var i = 0; i < 10; i++) {
      db.begin();
      EntityImpl doc = db.newInstance(className1);
      doc.setProperty("name", "name" + i);
      doc.setProperty("surname", "surname" + i);
      doc.save();
      db.commit();
    }

    db.begin();
    var result =
        db.command("insert into " + className2 + " ( select from " + className1 + ")");
    db.commit();

    printExecutionPlan(result);

    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());

    Set<String> names = new HashSet<>();
    for (var i = 0; i < 10; i++) {
      names.add("name" + i);
    }
    result = db.query("select from " + className2);
    for (var i = 0; i < 10; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotNull(item.getProperty("name"));
      names.remove(item.getProperty("name"));
      Assert.assertNotNull(item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    Assert.assertTrue(names.isEmpty());
    result.close();
  }

  @Test
  public void testContent() {
    var className = "testContent";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command("insert into " + className + " content {'name':'name1', 'surname':'surname1'}");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContentMultiple() {
    var className = "testContent";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command(
            "insert into "
                + className
                + " content {'name':'name1', 'surname':'surname1'},{'name':'name1',"
                + " 'surname':'surname1'}");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testContentWithParam() {
    var className = "testContentWithParam";
    db.getMetadata().getSchema().createClass(className);

    Map<String, Object> theContent = new HashMap<>();
    theContent.put("name", "name1");
    theContent.put("surname", "surname1");
    Map<String, Object> params = new HashMap<>();
    params.put("theContent", theContent);

    db.begin();
    var result = db.command("insert into " + className + " content :theContent", params);
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
      Assert.assertEquals("surname1", item.getProperty("surname"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkConversion() {
    var className1 = "testLinkConversion1";
    var className2 = "testLinkConversion2";

    db.command("CREATE CLASS " + className1).close();

    db.begin();
    db.command("INSERT INTO " + className1 + " SET name='Active';").close();
    db.command("INSERT INTO " + className1 + " SET name='Inactive';").close();
    db.commit();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".processingType LINK " + className1 + ";")
        .close();

    db.begin();
    db.command(
            "INSERT INTO "
                + className2
                + " SET name='Active', processingType = (SELECT FROM "
                + className1
                + " WHERE name = 'Active') ;")
        .close();
    db.command(
            "INSERT INTO "
                + className2
                + " SET name='Inactive', processingType = (SELECT FROM "
                + className1
                + " WHERE name = 'Inactive') ;")
        .close();
    db.commit();

    var result = db.query("SELECT FROM " + className2);
    for (var i = 0; i < 2; i++) {
      Assert.assertTrue(result.hasNext());
      var row = result.next();
      var val = row.getProperty("processingType");
      Assert.assertNotNull(val);
      Assert.assertTrue(val instanceof Identifiable);
    }
    result.close();
  }

  @Test
  public void testEmbeddedlistConversion() {
    var className1 = "testEmbeddedlistConversion1";
    var className2 = "testEmbeddedlistConversion2";

    db.command("CREATE CLASS " + className1).close();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".sub EMBEDDEDLIST " + className1 + ";").close();

    db.begin();
    db.command("INSERT INTO " + className2 + " SET name='Active', sub = [{'name':'foo'}];").close();
    db.commit();

    var result = db.query("SELECT FROM " + className2);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var row = result.next();
      var list = row.getProperty("sub");
      Assert.assertNotNull(list);
      Assert.assertTrue(list instanceof List);
      Assert.assertEquals(1, ((List) list).size());

      var o = ((List) list).get(0);
      Assert.assertTrue(o instanceof Result);
      Assert.assertEquals("foo", ((Result) o).getProperty("name"));
      Assert.assertEquals(className1, ((Result) o).asEntity().getSchemaType().get().getName());
    }
    result.close();
  }

  @Test
  public void testEmbeddedlistConversion2() {
    var className1 = "testEmbeddedlistConversion21";
    var className2 = "testEmbeddedlistConversion22";

    db.command("CREATE CLASS " + className1).close();

    db.command("CREATE CLASS " + className2 + ";").close();
    db.command("CREATE PROPERTY " + className2 + ".sub EMBEDDEDLIST " + className1 + ";").close();

    db.begin();
    db.command("INSERT INTO " + className2 + " (name, sub) values ('Active', [{'name':'foo'}]);")
        .close();
    db.commit();

    var result = db.query("SELECT FROM " + className2);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var row = result.next();
      var list = row.getProperty("sub");
      Assert.assertNotNull(list);
      Assert.assertTrue(list instanceof List);
      Assert.assertEquals(1, ((List) list).size());

      var o = ((List) list).get(0);
      Assert.assertTrue(o instanceof Result);
      Assert.assertEquals("foo", ((Result) o).getProperty("name"));
      Assert.assertEquals(className1, ((Result) o).asEntity().getSchemaType().get().getName());
    }
    result.close();
  }

  @Test
  public void testInsertReturn() {
    var className = "testInsertReturn";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command("insert into " + className + " set name = 'name1' RETURN 'OK' as result");
    db.commit();

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("OK", item.getProperty("result"));
    }
    Assert.assertFalse(result.hasNext());

    result = db.query("select from " + className);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals("name1", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testNestedInsert() {
    var className = "testNestedInsert";
    db.getMetadata().getSchema().createClass(className);

    db.begin();
    var result =
        db.command(
            "insert into "
                + className
                + " set name = 'parent', children = (INSERT INTO "
                + className
                + " SET name = 'child')");
    db.commit();

    result.close();

    result = db.query("SELECT FROM " + className);

    for (var i = 0; i < 2; i++) {
      var item = result.next();
      if (item.getProperty("name").equals("parent")) {
        Assert.assertTrue(item.getProperty("children") instanceof Collection);
        Assert.assertEquals(1, ((Collection) item.getProperty("children")).size());
      }
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLinkMapWithSubqueries() {
    var className = "testLinkMapWithSubqueries";
    var itemclassName = "testLinkMapWithSubqueriesTheItem";

    db.command("CREATE CLASS " + className);
    db.command("CREATE CLASS " + itemclassName);
    db.command("CREATE PROPERTY " + className + ".mymap LINKMAP " + itemclassName);

    db.begin();
    db.command("INSERT INTO " + itemclassName + " (name) VALUES ('test')");
    db.command(
        "INSERT INTO "
            + className
            + " (mymap) VALUES ({'A-1': (SELECT FROM "
            + itemclassName
            + " WHERE name = 'test')})");
    db.commit();

    var result = db.query("SELECT FROM " + className);

    var item = result.next();
    Map theMap = item.getProperty("mymap");
    Assert.assertEquals(1, theMap.size());
    Assert.assertNotNull(theMap.get("A-1"));

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testQuotedCharactersInJson() {
    var className = "testQuotedCharactersInJson";

    db.command("CREATE CLASS " + className);

    db.begin();
    db.command(
        "INSERT INTO "
            + className
            + " CONTENT { name: \"jack\", memo: \"this is a \\n multi line text\" }");
    db.commit();

    var result = db.query("SELECT FROM " + className);

    var item = result.next();
    String memo = item.getProperty("memo");
    Assert.assertEquals("this is a \n multi line text", memo);

    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testInsertIndexTest() {
    db.command("CREATE INDEX testInsert UNIQUE STRING ");

    db.begin();
    try (var insert = db.command("INSERT INTO index:testInsert set key='one', rid=#5:0")) {
      assertEquals((long) insert.next().getProperty("count"), 1L);
      assertFalse(insert.hasNext());
    }
    db.commit();

    try (var result = db.query("SELECT FROM index:testInsert ")) {
      var item = result.next();
      assertEquals(item.getProperty("key"), "one");
      assertEquals(item.getProperty("rid"), new RecordId(5, 0));
      assertFalse(result.hasNext());
    }
  }
}
