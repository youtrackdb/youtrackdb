package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.List;
import org.junit.Test;

public class UpdateWithRidParameters extends DBTestBase {

  @Test
  public void testRidParameters() {

    YTSchema schm = db.getMetadata().getSchema();
    schm.createClass("testingClass");
    schm.createClass("testingClass2");

    db.command("INSERT INTO testingClass SET id = ?", 123).close();

    db.command("INSERT INTO testingClass2 SET id = ?", 456).close();
    YTRID orid;
    try (YTResultSet docs = db.query("SELECT FROM testingClass2 WHERE id = ?", 456)) {
      orid = docs.next().getProperty("@rid");
    }

    // This does not work. It silently adds a null instead of the YTRID.
    db.command("UPDATE testingClass set linkedlist = linkedlist || ?", orid).close();

    // This does work.
    db.command("UPDATE testingClass set linkedlist = linkedlist || " + orid.toString()).close();
    List<YTRID> lst;
    try (YTResultSet docs = db.query("SELECT FROM testingClass WHERE id = ?", 123)) {
      lst = docs.next().getProperty("linkedlist");
    }

    assertEquals(orid, lst.get(0));
    assertEquals(orid, lst.get(1));
  }
}
