package com.jetbrains.youtrack.db.internal.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Test;

/**
 *
 */
public class TestNullFieldQuery extends DBTestBase {

  @Test
  public void testQueryNullValue() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "name", YTType.ANY);

    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }
}
