package com.orientechnologies.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSchemaException;
import org.junit.Test;

public class AlterClassClusterTest extends DBTestBase {

  @Test
  public void testRemoveClusterDefaultCluster() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.addCluster(db, "TestOneMore");

    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
    clazz = db.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getDefaultClusterId(), db.getClusterIdByName("TestOneMore"));
  }

  @Test(expected = YTDatabaseException.class)
  public void testRemoveLastClassCluster() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.removeClusterId(db, db.getClusterIdByName("Test"));
  }

  @Test(expected = YTSchemaException.class)
  public void testAddClusterToAbstracClass() {
    YTClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster(db, "TestOneMore");
  }

  @Test(expected = YTSchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    YTClass clazz = db.getMetadata().getSchema().createAbstractClass("Test");
    int id = db.addCluster("TestOneMore");
    clazz.addClusterId(db, id);
  }

  @Test
  public void testSetAbstractRestrictedClass() {
    YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oRestricted = oSchema.getClass("ORestricted");
    YTClass v = oSchema.getClass("V");
    v.addSuperClass(db, oRestricted);

    YTClass ovt = oSchema.createClass("Some", v);
    ovt.setAbstract(db, true);
    assertTrue(ovt.isAbstract());
  }
}
