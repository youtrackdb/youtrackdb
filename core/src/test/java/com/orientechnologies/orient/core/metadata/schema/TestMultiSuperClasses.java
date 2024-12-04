package com.orientechnologies.orient.core.metadata.schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.exception.OSchemaException;
import java.util.Collections;
import java.util.List;
import org.junit.Test;

public class TestMultiSuperClasses extends BaseMemoryInternalDatabase {

  @Test
  public void testClassCreation() {
    YTSchema oSchema = db.getMetadata().getSchema();

    YTClass aClass = oSchema.createAbstractClass("javaA");
    YTClass bClass = oSchema.createAbstractClass("javaB");
    aClass.createProperty(db, "propertyInt", YTType.INTEGER);
    bClass.createProperty(db, "propertyDouble", YTType.DOUBLE);
    YTClass cClass = oSchema.createClass("javaC", aClass, bClass);
    testClassCreationBranch(aClass, bClass, cClass);
    testClassCreationBranch(aClass, bClass, cClass);
    oSchema = db.getMetadata().getImmutableSchemaSnapshot();
    aClass = oSchema.getClass("javaA");
    bClass = oSchema.getClass("javaB");
    cClass = oSchema.getClass("javaC");
    testClassCreationBranch(aClass, bClass, cClass);
  }

  private void testClassCreationBranch(YTClass aClass, YTClass bClass, YTClass cClass) {
    assertNotNull(aClass.getSuperClasses());
    assertEquals(aClass.getSuperClasses().size(), 0);
    assertNotNull(bClass.getSuperClassesNames());
    assertEquals(bClass.getSuperClassesNames().size(), 0);
    assertNotNull(cClass.getSuperClassesNames());
    assertEquals(cClass.getSuperClassesNames().size(), 2);

    List<? extends YTClass> superClasses = cClass.getSuperClasses();
    assertTrue(superClasses.contains(aClass));
    assertTrue(superClasses.contains(bClass));
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    assertTrue(aClass.isSuperClassOf(cClass));
    assertTrue(bClass.isSuperClassOf(cClass));

    YTProperty property = cClass.getProperty("propertyInt");
    assertEquals(YTType.INTEGER, property.getType());
    property = cClass.propertiesMap(db).get("propertyInt");
    assertEquals(YTType.INTEGER, property.getType());

    property = cClass.getProperty("propertyDouble");
    assertEquals(YTType.DOUBLE, property.getType());
    property = cClass.propertiesMap(db).get("propertyDouble");
    assertEquals(YTType.DOUBLE, property.getType());
  }

  @Test
  public void testSql() {
    final YTSchema oSchema = db.getMetadata().getSchema();

    YTClass aClass = oSchema.createAbstractClass("sqlA");
    YTClass bClass = oSchema.createAbstractClass("sqlB");
    YTClass cClass = oSchema.createClass("sqlC");
    db.command("alter class sqlC superclasses sqlA, sqlB").close();
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    db.command("alter class sqlC superclass sqlA").close();
    assertTrue(cClass.isSubClassOf(aClass));
    assertFalse(cClass.isSubClassOf(bClass));
    db.command("alter class sqlC superclass +sqlB").close();
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
    db.command("alter class sqlC superclass -sqlA").close();
    assertFalse(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
  }

  @Test
  public void testCreationBySql() {
    final YTSchema oSchema = db.getMetadata().getSchema();

    db.command("create class sql2A abstract").close();
    db.command("create class sql2B abstract").close();
    db.command("create class sql2C extends sql2A, sql2B abstract").close();

    YTClass aClass = oSchema.getClass("sql2A");
    YTClass bClass = oSchema.getClass("sql2B");
    YTClass cClass = oSchema.getClass("sql2C");
    assertNotNull(aClass);
    assertNotNull(bClass);
    assertNotNull(cClass);
    assertTrue(cClass.isSubClassOf(aClass));
    assertTrue(cClass.isSubClassOf(bClass));
  }

  @Test(
      expected = OSchemaException.class) // , expectedExceptionsMessageRegExp = "(?s).*recursion.*"
  // )
  public void testPreventionOfCycles() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass aClass = oSchema.createAbstractClass("cycleA");
    YTClass bClass = oSchema.createAbstractClass("cycleB", aClass);
    YTClass cClass = oSchema.createAbstractClass("cycleC", bClass);

    aClass.setSuperClasses(db, Collections.singletonList(cClass));
  }

  @Test
  public void testParametersImpactGoodScenario() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass aClass = oSchema.createAbstractClass("impactGoodA");
    aClass.createProperty(db, "property", YTType.STRING);
    YTClass bClass = oSchema.createAbstractClass("impactGoodB");
    bClass.createProperty(db, "property", YTType.STRING);
    YTClass cClass = oSchema.createAbstractClass("impactGoodC", aClass, bClass);
    assertTrue(cClass.existsProperty("property"));
  }

  @Test(
      expected = OSchemaException.class) // }, expectedExceptionsMessageRegExp = "(?s).*conflict.*")
  public void testParametersImpactBadScenario() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass aClass = oSchema.createAbstractClass("impactBadA");
    aClass.createProperty(db, "property", YTType.STRING);
    YTClass bClass = oSchema.createAbstractClass("impactBadB");
    bClass.createProperty(db, "property", YTType.INTEGER);
    oSchema.createAbstractClass("impactBadC", aClass, bClass);
  }

  @Test
  public void testCreationOfClassWithV() {
    final YTSchema oSchema = db.getMetadata().getSchema();
    YTClass oRestrictedClass = oSchema.getClass("ORestricted");
    YTClass vClass = oSchema.getClass("V");
    vClass.setSuperClasses(db, Collections.singletonList(oRestrictedClass));
    YTClass dummy1Class = oSchema.createClass("Dummy1", oRestrictedClass, vClass);
    YTClass dummy2Class = oSchema.createClass("Dummy2");
    YTClass dummy3Class = oSchema.createClass("Dummy3", dummy1Class, dummy2Class);
    assertNotNull(dummy3Class);
  }
}
