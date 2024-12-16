package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Test;

/**
 *
 */
public class TestNullFieldQuery extends DbTestBase {

  @Test
  public void testQueryNullValue() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "name", PropertyType.ANY);

    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }
}
