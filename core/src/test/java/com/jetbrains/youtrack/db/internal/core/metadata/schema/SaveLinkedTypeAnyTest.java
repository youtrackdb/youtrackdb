package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class SaveLinkedTypeAnyTest extends DbTestBase {

  @Test
  public void testRemoveLinkedType() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass classA = schema.createClass("TestRemoveLinkedType");
    classA.createProperty(db, "prop", PropertyType.EMBEDDEDLIST, PropertyType.ANY);

    db.begin();
    db.command("insert into TestRemoveLinkedType set prop = [4]").close();
    db.commit();

    try (ResultSet result = db.query("select from TestRemoveLinkedType")) {
      Assert.assertTrue(result.hasNext());
      Collection coll = result.next().getProperty("prop");
      Assert.assertFalse(result.hasNext());
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(coll.iterator().next(), 4);
    }
  }

  @Test
  public void testAlterRemoveLinkedType() {
    Schema schema = db.getMetadata().getSchema();
    SchemaClass classA = schema.createClass("TestRemoveLinkedType");
    classA.createProperty(db, "prop", PropertyType.EMBEDDEDLIST, PropertyType.ANY);

    db.command("alter property TestRemoveLinkedType.prop linkedtype null").close();

    db.begin();
    db.command("insert into TestRemoveLinkedType set prop = [4]").close();
    db.commit();

    try (ResultSet result = db.query("select from TestRemoveLinkedType")) {
      Assert.assertTrue(result.hasNext());
      Collection coll = result.next().getProperty("prop");
      Assert.assertFalse(result.hasNext());
      Assert.assertEquals(coll.size(), 1);
      Assert.assertEquals(coll.iterator().next(), 4);
    }
  }
}
