package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.exception.OSchemaException;
import org.junit.Test;

public class AlterClassClusterTest extends DBTestBase {

  @Test
  public void testRemoveClusterDefaultCluster() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.addCluster(db, "TestOneMore");

    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
    clazz = db.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getDefaultClusterId(), db.getClusterIdByName("TestOneMore"));
  }

  @Test(expected = ODatabaseException.class)
  public void testRemoveLastClassCluster() {
    OClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
  }

  @Test(expected = OSchemaException.class)
  public void testAddClusterToAbstracClass() {
    OClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster(db, "TestOneMore");
  }

  @Test(expected = OSchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    OClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    int id = db.addCluster("TestOneMore");
    clazz.addClusterId(db, id);
  }

  @Test
  public void testSetAbstractRestrictedClass() {
    OSchema oSchema = db.getMetadata().getSchema();
    OClass oRestricted = oSchema.getClass("ORestricted");
    OClass v = oSchema.getClass("V");
    v.addSuperClass(db, oRestricted);

    OClass ovt = oSchema.createClass("Some", v);
    ovt.setAbstract(db, true);
    assertTrue(ovt.isAbstract());
  }
}
