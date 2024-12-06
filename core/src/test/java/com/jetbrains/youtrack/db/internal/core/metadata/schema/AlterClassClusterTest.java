package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.SchemaException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import org.junit.Test;

public class AlterClassClusterTest extends DbTestBase {

  @Test
  public void testRemoveClusterDefaultCluster() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.addCluster(db, "TestOneMore");

    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
    clazz = db.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getDefaultClusterId(), db.getClusterIdByName("TestOneMore"));
  }

  @Test(expected = DatabaseException.class)
  public void testRemoveLastClassCluster() {
    SchemaClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterToAbstracClass() {
    SchemaClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster(db, "TestOneMore");
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    SchemaClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    int id = db.addCluster("TestOneMore");
    clazz.addClusterId(db, id);
  }

  @Test
  public void testSetAbstractRestrictedClass() {
    Schema oSchema = db.getMetadata().getSchema();
    SchemaClass oRestricted = oSchema.getClass("ORestricted");
    SchemaClass v = oSchema.getClass("V");
    v.addSuperClass(db, oRestricted);

    SchemaClass ovt = oSchema.createClass("Some", v);
    ovt.setAbstract(db, true);
    assertTrue(ovt.isAbstract());
  }
}
