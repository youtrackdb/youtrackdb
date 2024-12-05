package com.jetbrains.youtrack.db.internal.core.sql.functions.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.exception.YTValidationException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OSqlUpdateContentValidationTest extends DBTestBase {

  @Test
  public void testReadOnlyValidation() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "testNormal", YTType.STRING);
    clazz.createProperty(db, "test", YTType.STRING).setReadonly(db, true);

    db.begin();
    YTResultSet res =
        db.command("insert into Test content {\"testNormal\":\"hello\",\"test\":\"only read\"} ");
    db.commit();
    YTIdentifiable id = res.next().getProperty("@rid");
    try {
      db.begin();
      db.command("update " + id + " CONTENT {\"testNormal\":\"by\"}").close();
      db.commit();
      Assert.fail("Error on update of a record removing a readonly property");
    } catch (YTValidationException val) {

    }
  }
}
