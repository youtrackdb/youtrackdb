package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static com.jetbrains.youtrack.db.internal.core.sql.executor.ExecutionPlanPrintUtils.printExecutionPlan;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class DeleteStatementExecutionTest extends DbTestBase {

  @Test
  public void testSimple() {
    var className = "testSimple";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
      session.commit();
    }

    session.begin();
    var result = session.command("delete from  " + className + " where name = 'name4'");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result = session.query("select from " + className);
    for (var i = 0; i < 9; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotEquals("name4", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testUnsafe1() {
    var className = "testUnsafe1";
    var v = session.getMetadata().getSchema().getClass("V");
    if (v == null) {
      session.getMetadata().getSchema().createClass("V");
    }
    session.getMetadata().getSchema().createClass(className, v);
    for (var i = 0; i < 10; i++) {
      session.begin();
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
      session.commit();
    }
    try {
      var result = session.command("delete from  " + className + " where name = 'name4'");
      Assert.fail();
    } catch (CommandExecutionException ex) {

    } catch (Exception e) {
      Assert.fail();
    }
  }

  @Test
  public void testUnsafe2() {
    var className = "testUnsafe2";
    var v = session.getMetadata().getSchema().getClass("V");
    if (v == null) {
      session.getMetadata().getSchema().createClass("V");
    }
    session.getMetadata().getSchema().createClass(className, v);
    for (var i = 0; i < 10; i++) {
      session.begin();
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
      session.commit();
    }

    session.begin();
    var result = session.command("delete from  " + className + " where name = 'name4' unsafe");

    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 1L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result = session.query("select from " + className);
    for (var i = 0; i < 9; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotEquals("name4", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testReturnBefore() {
    var className = "testReturnBefore";
    session.getMetadata().getSchema().createClass(className);
    RID fourthId = null;

    for (var i = 0; i < 10; i++) {
      session.begin();
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      if (i == 4) {
        fourthId = doc.getIdentity();
      }

      doc.save();
      session.commit();
    }

    session.begin();
    var result =
        session.command("delete from  " + className + " return before where name = 'name4' ");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals(fourthId, item.getRecordId());
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result = session.query("select from " + className);
    for (var i = 0; i < 9; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertNotEquals("name4", item.getProperty("name"));
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }

  @Test
  public void testLimit() {
    var className = "testLimit";
    session.getMetadata().getSchema().createClass(className);
    for (var i = 0; i < 10; i++) {
      session.begin();
      EntityImpl doc = session.newInstance(className);
      doc.setProperty("name", "name" + i);
      doc.save();
      session.commit();
    }
    session.begin();
    var result = session.command("delete from  " + className + " limit 5");
    printExecutionPlan(result);
    for (var i = 0; i < 1; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
      Assert.assertEquals((Object) 5L, item.getProperty("count"));
    }
    Assert.assertFalse(result.hasNext());
    session.commit();

    result = session.query("select from " + className);
    for (var i = 0; i < 5; i++) {
      Assert.assertTrue(result.hasNext());
      var item = result.next();
      Assert.assertNotNull(item);
    }
    Assert.assertFalse(result.hasNext());
    result.close();
  }
}
