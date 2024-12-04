package com.orientechnologies.orient.core.sql.functions.sql;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OSqlUpdateContentValidationTest extends DBTestBase {

  @Test
  public void testReadOnlyValidation() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "testNormal", YTType.STRING);
    clazz.createProperty(db, "test", YTType.STRING).setReadonly(db, true);

    db.begin();
    OResultSet res =
        db.command("insert into Test content {\"testNormal\":\"hello\",\"test\":\"only read\"} ");
    db.commit();
    YTIdentifiable id = res.next().getProperty("@rid");
    try {
      db.begin();
      db.command("update " + id + " CONTENT {\"testNormal\":\"by\"}").close();
      db.commit();
      Assert.fail("Error on update of a record removing a readonly property");
    } catch (OValidationException val) {

    }
  }
}
