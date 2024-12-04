package com.orientechnologies.orient.core.sql.select;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

/**
 *
 */
public class TestNullFieldQuery extends DBTestBase {

  @Test
  public void testQueryNullValue() {
    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    YTDocument doc = new YTDocument("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    OResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }

  @Test
  public void testQueryNullValueSchemaFull() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "name", YTType.ANY);

    db.begin();
    YTDocument doc = new YTDocument("Test");
    doc.field("name", (Object) null);
    db.save(doc);
    db.commit();

    OResultSet res = db.query("select from Test where name= 'some' ");
    assertEquals(0, res.stream().count());
  }
}
