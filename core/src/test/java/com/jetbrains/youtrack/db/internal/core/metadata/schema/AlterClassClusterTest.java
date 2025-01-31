package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

public class AlterClassClusterTest extends DbTestBase {

  @Test
  public void testRemoveClusterDefaultCluster() {
    var clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.addCluster(db, "TestOneMore");

    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
    clazz = db.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getClusterIds()[0], db.getClusterIdByName("TestOneMore"));
  }

  @Test(expected = DatabaseException.class)
  public void testRemoveLastClassCluster() {
    var clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterToAbstracClass() {
    var clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster(db, "TestOneMore");
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    var clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    var id = db.addCluster("TestOneMore");
    clazz.addClusterId(db, id);
  }

  @Test
  public void testSetAbstractRestrictedClass() {
    Schema oSchema = db.getMetadata().getSchema();
    var oRestricted = oSchema.getClass("ORestricted");
    var v = oSchema.getClass("V");
    v.addSuperClass(db, oRestricted);

    var ovt = oSchema.createClass("Some", v);
    ovt.setAbstract(db, true);
    assertTrue(ovt.isAbstract());
  }
}
