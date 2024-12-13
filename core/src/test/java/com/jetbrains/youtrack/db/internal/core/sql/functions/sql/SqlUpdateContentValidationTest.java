package com.jetbrains.youtrack.db.internal.core.sql.functions.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class SqlUpdateContentValidationTest extends DbTestBase {

  @Test
  public void testReadOnlyValidation() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "testNormal", PropertyType.STRING);
    clazz.createProperty(db, "test", PropertyType.STRING).setReadonly(db, true);

    db.begin();
    ResultSet res =
        db.command("insert into Test content {\"testNormal\":\"hello\",\"test\":\"only read\"} ");
    db.commit();
    Identifiable id = res.next().getProperty("@rid");
    try {
      db.begin();
      db.command("update " + id + " CONTENT {\"testNormal\":\"by\"}").close();
      db.commit();
      Assert.fail("Error on update of a record removing a readonly property");
    } catch (ValidationException val) {

    }
  }
}
