package com.orientechnologies.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResultSet;
import org.junit.Test;

/**
 *
 */
public class TestNullFieldQuery extends DBTestBase {

  @Test
  public void testQueryNullValue() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
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
    YTEntityImpl doc = new YTEntityImpl("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }
}
