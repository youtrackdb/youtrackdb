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
    var clazz = session.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.addCluster(session, "TestOneMore");

    clazz.removeClusterId(session, session.getClusterIdByName("Test"));
    clazz = session.getMetadata().getSchema().getClass("Test");
    assertEquals(clazz.getClusterIds(session)[0], session.getClusterIdByName("TestOneMore"));
  }

  @Test(expected = DatabaseException.class)
  public void testRemoveLastClassCluster() {
    var clazz = session.getMetadata().getSchema().createClass("Test", 1, null);
    clazz.removeClusterId(session, session.getClusterIdByName("Test"));
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterToAbstracClass() {
    var clazz = session.getMetadata().getSchema().createAbstractClass("Test");
    clazz.addCluster(session, "TestOneMore");
  }

  @Test(expected = SchemaException.class)
  public void testAddClusterIdToAbstracClass() {
    var clazz = session.getMetadata().getSchema().createAbstractClass("Test");
    var id = session.addCluster("TestOneMore");
    clazz.addClusterId(session, id);
  }

  @Test
  public void testSetAbstractRestrictedClass() {
    Schema oSchema = session.getMetadata().getSchema();
    var oRestricted = oSchema.getClass("ORestricted");
    var v = oSchema.getClass("V");
    v.addSuperClass(session, oRestricted);

    var ovt = oSchema.createClass("Some", v);
    ovt.setAbstract(session, true);
    assertTrue(ovt.isAbstract(session));
  }
}
