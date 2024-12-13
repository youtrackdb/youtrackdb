package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import java.util.List;
import org.junit.Test;

public class UpdateWithRidParameters extends DbTestBase {

  @Test
  public void testRidParameters() {

    Schema schm = db.getMetadata().getSchema();
    schm.createClass("testingClass");
    schm.createClass("testingClass2");

    db.command("INSERT INTO testingClass SET id = ?", 123).close();

    db.command("INSERT INTO testingClass2 SET id = ?", 456).close();
    RID orid;
    try (ResultSet docs = db.query("SELECT FROM testingClass2 WHERE id = ?", 456)) {
      orid = docs.next().getProperty("@rid");
    }

    // This does not work. It silently adds a null instead of the RID.
    db.command("UPDATE testingClass set linkedlist = linkedlist || ?", orid).close();

    // This does work.
    db.command("UPDATE testingClass set linkedlist = linkedlist || " + orid.toString()).close();
    List<RID> lst;
    try (ResultSet docs = db.query("SELECT FROM testingClass WHERE id = ?", 123)) {
      lst = docs.next().getProperty("linkedlist");
    }

    assertEquals(orid, lst.get(0));
    assertEquals(orid, lst.get(1));
  }
}
