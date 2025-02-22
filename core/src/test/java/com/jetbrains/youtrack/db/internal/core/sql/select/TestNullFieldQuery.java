package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

/**
 *
 */
public class TestNullFieldQuery extends DbTestBase {

  @Test
  public void testQueryNullValue() {
    session.getMetadata().getSchema().createClass("Test");
    session.begin();
    var doc = (EntityImpl) session.newEntity("Test");
    doc.field("name", null);
    session.commit();

    var res = session.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    var clazz = session.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(session, "name", PropertyType.ANY);

    session.begin();
    var doc = (EntityImpl) session.newEntity("Test");
    doc.field("name", null);
    session.commit();

    var res = session.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }
}
