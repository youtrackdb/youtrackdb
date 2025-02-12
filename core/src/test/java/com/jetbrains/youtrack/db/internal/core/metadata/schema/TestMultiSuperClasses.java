package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.exception.SchemaException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class TestMultiSuperClasses extends BaseMemoryInternalDatabase {

  @Test
  public void testClassCreation() {
    Schema oSchema = session.getMetadata().getSchema();

    var aClass = oSchema.createAbstractClass("javaA");
    var bClass = oSchema.createAbstractClass("javaB");
    aClass.createProperty(session, "propertyInt", PropertyType.INTEGER);
    bClass.createProperty(session, "propertyDouble", PropertyType.DOUBLE);
    var cClass = oSchema.createClass("javaC", aClass, bClass);
    testClassCreationBranch(aClass, bClass, cClass);
    testClassCreationBranch(aClass, bClass, cClass);
    oSchema = session.getMetadata().getImmutableSchemaSnapshot();
    aClass = oSchema.getClass("javaA");
    bClass = oSchema.getClass("javaB");
    cClass = oSchema.getClass("javaC");
    testClassCreationBranch(aClass, bClass, cClass);
  }

  private void testClassCreationBranch(SchemaClass aClass, SchemaClass bClass, SchemaClass cClass) {
    assertNotNull(aClass.getSuperClasses(session));
    assertEquals(0, aClass.getSuperClasses(session).size());
    assertNotNull(bClass.getSuperClassesNames(session));
    assertEquals(0, bClass.getSuperClassesNames(session).size());
    assertNotNull(cClass.getSuperClassesNames(session));
    assertEquals(2, cClass.getSuperClassesNames(session).size());

    List<? extends SchemaClass> superClasses = cClass.getSuperClasses(session);
    assertTrue(superClasses.contains(aClass));
    assertTrue(superClasses.contains(bClass));
    assertTrue(cClass.isSubClassOf(session, aClass));
    assertTrue(cClass.isSubClassOf(session, bClass));
    assertTrue(aClass.isSuperClassOf(session, cClass));
    assertTrue(bClass.isSuperClassOf(session, cClass));

    var property = cClass.getProperty(session, "propertyInt");
    assertEquals(PropertyType.INTEGER, property.getType(session));
    property = cClass.propertiesMap(session).get("propertyInt");
    assertEquals(PropertyType.INTEGER, property.getType(session));

    property = cClass.getProperty(session, "propertyDouble");
    assertEquals(PropertyType.DOUBLE, property.getType(session));
    property = cClass.propertiesMap(session).get("propertyDouble");
    assertEquals(PropertyType.DOUBLE, property.getType(session));
  }

  @Test
  public void testSql() {
    final Schema oSchema = session.getMetadata().getSchema();

    var aClass = oSchema.createAbstractClass("sqlA");
    var bClass = oSchema.createAbstractClass("sqlB");
    var cClass = oSchema.createClass("sqlC");
    session.command("alter class sqlC superclasses sqlA, sqlB").close();
    assertTrue(cClass.isSubClassOf(session, aClass));
    assertTrue(cClass.isSubClassOf(session, bClass));
    session.command("alter class sqlC superclass sqlA").close();
    assertTrue(cClass.isSubClassOf(session, aClass));
    assertFalse(cClass.isSubClassOf(session, bClass));
    session.command("alter class sqlC superclass +sqlB").close();
    assertTrue(cClass.isSubClassOf(session, aClass));
    assertTrue(cClass.isSubClassOf(session, bClass));
    session.command("alter class sqlC superclass -sqlA").close();
    assertFalse(cClass.isSubClassOf(session, aClass));
    assertTrue(cClass.isSubClassOf(session, bClass));
  }

  @Test
  public void testCreationBySql() {
    final Schema oSchema = session.getMetadata().getSchema();

    session.command("create class sql2A abstract").close();
    session.command("create class sql2B abstract").close();
    session.command("create class sql2C extends sql2A, sql2B abstract").close();

    var aClass = oSchema.getClass("sql2A");
    var bClass = oSchema.getClass("sql2B");
    var cClass = oSchema.getClass("sql2C");
    assertNotNull(aClass);
    assertNotNull(bClass);
    assertNotNull(cClass);
    assertTrue(cClass.isSubClassOf(session, aClass));
    assertTrue(cClass.isSubClassOf(session, bClass));
  }

  @Test(
      expected = SchemaException.class) // , expectedExceptionsMessageRegExp = "(?s).*recursion.*"
  // )
  public void testPreventionOfCycles() {
    final Schema oSchema = session.getMetadata().getSchema();
    var aClass = oSchema.createAbstractClass("cycleA");
    var bClass = oSchema.createAbstractClass("cycleB", aClass);
    var cClass = oSchema.createAbstractClass("cycleC", bClass);

    aClass.setSuperClasses(session, Collections.singletonList(cClass));
  }

  @Test
  public void testParametersImpactGoodScenario() {
    final Schema oSchema = session.getMetadata().getSchema();
    var aClass = oSchema.createAbstractClass("impactGoodA");
    aClass.createProperty(session, "property", PropertyType.STRING);
    var bClass = oSchema.createAbstractClass("impactGoodB");
    bClass.createProperty(session, "property", PropertyType.STRING);
    var cClass = oSchema.createAbstractClass("impactGoodC", aClass, bClass);
    assertTrue(cClass.existsProperty(session, "property"));
  }

  @Test(
      expected = SchemaException.class)
  // }, expectedExceptionsMessageRegExp = "(?s).*conflict.*")
  public void testParametersImpactBadScenario() {
    final Schema oSchema = session.getMetadata().getSchema();
    var aClass = oSchema.createAbstractClass("impactBadA");
    aClass.createProperty(session, "property", PropertyType.STRING);
    var bClass = oSchema.createAbstractClass("impactBadB");
    bClass.createProperty(session, "property", PropertyType.INTEGER);
    oSchema.createAbstractClass("impactBadC", aClass, bClass);
  }

  @Test
  public void testCreationOfClassWithV() {
    final Schema oSchema = session.getMetadata().getSchema();
    var oRestrictedClass = oSchema.getClass("ORestricted");
    var vClass = oSchema.getClass("V");
    vClass.setSuperClasses(session, Collections.singletonList(oRestrictedClass));
    var dummy1Class = oSchema.createClass("Dummy1", oRestrictedClass, vClass);
    var dummy2Class = oSchema.createClass("Dummy2");
    var dummy3Class = oSchema.createClass("Dummy3", dummy1Class, dummy2Class);
    assertNotNull(dummy3Class);
  }
}
