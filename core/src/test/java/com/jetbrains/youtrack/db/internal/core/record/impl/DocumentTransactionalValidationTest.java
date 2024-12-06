package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.exception.ValidationException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Direction;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DocumentTransactionalValidationTest extends BaseMemoryInternalDatabase {

  @Test(expected = ValidationException.class)
  public void simpleConstraintShouldBeCheckedOnCommitFalseTest() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.save();
    db.commit();
  }

  @Test()
  public void simpleConstraintShouldBeCheckedOnCommitTrueTest() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("int", 11);
    vertex.save();
    db.commit();
    db.begin();
    db.begin();
    Integer value = db.bindToSession(vertex).getProperty("int");
    Assert.assertEquals((Integer) 11, value);
  }

  @Test()
  public void simpleConstraintShouldBeCheckedOnCommitWithTypeConvert() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("int", "11");
    vertex.save();
    db.commit();
    db.begin();
    Integer value = db.bindToSession(vertex).getProperty("int");
    Assert.assertEquals((Integer) 11, value);
  }

  @Test
  public void stringRegexpPatternValidationCheck() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "str", PropertyType.STRING).setMandatory(db, true)
        .setRegexp(db, "aba.*");
    Vertex vertex;
    db.begin();
    vertex = db.newVertex(clazz.getName());
    vertex.setProperty("str", "first");
    vertex.setProperty("str", "second");
    vertex.save();
    vertex.setProperty("str", "abacorrect");
    db.commit();
    Assert.assertEquals("abacorrect", db.bindToSession(vertex).getProperty("str"));
  }

  @Test(expected = ValidationException.class)
  public void stringRegexpPatternValidationCheckFails() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "str", PropertyType.STRING).setMandatory(db, true)
        .setRegexp(db, "aba.*");
    Vertex vertex;
    db.begin();
    vertex = db.newVertex(clazz.getName());
    vertex.setProperty("str", "first");
    vertex.save();
    db.commit();
  }

  @Test(expected = ValidationException.class)
  public void requiredLinkBagNegativeTest() {
    SchemaClass edgeClass = db.createEdgeClass("lst");
    SchemaClass clazz = db.createVertexClass("Validation");
    SchemaClass linkClass = db.createVertexClass("links");
    String edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    db.newVertex(clazz.getName()).save();
    db.commit();
  }

  @Test
  public void requiredLinkBagPositiveTest() {
    SchemaClass edgeClass = db.createLightweightEdgeClass("lst");
    SchemaClass clazz = db.createVertexClass("Validation");
    SchemaClass linkClass = db.createVertexClass("links");
    String edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    Vertex vrt = db.newVertex(clazz.getName());
    Vertex link = db.newVertex(linkClass.getName());
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
  }

  @Test(expected = ValidationException.class)
  public void requiredLinkBagFailsIfBecomesEmpty() {
    SchemaClass edgeClass = db.createEdgeClass("lst");
    SchemaClass clazz = db.createVertexClass("Validation");
    SchemaClass linkClass = db.createVertexClass("links");
    String edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true)
        .setMin(db, "1");
    db.begin();
    Vertex vrt = db.newVertex(clazz.getName());
    Vertex link = db.newVertex(linkClass.getName());
    vrt.addEdge(link, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt.getEdges(Direction.OUT, edgeClass).forEach(Entity::delete);
    vrt.save();
    db.commit();
  }

  @Test(expected = ValidationException.class)
  public void requiredArrayFailsIfBecomesEmpty() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "arr", PropertyType.EMBEDDEDLIST).setMandatory(db, true)
        .setMin(db, "1");
    db.begin();
    Vertex vrt = db.newVertex(clazz.getName());
    vrt.setProperty("arr", Arrays.asList(1, 2, 3));
    vrt.save();
    db.commit();
    db.begin();
    vrt = db.bindToSession(vrt);
    List<Integer> arr = vrt.getProperty("arr");
    arr.clear();
    vrt.save();
    db.commit();
  }

  @Test
  public void requiredLinkBagCanBeEmptyDuringTransaction() {
    SchemaClass edgeClass = db.createLightweightEdgeClass("lst");
    SchemaClass clazz = db.createVertexClass("Validation");
    SchemaClass linkClass = db.createVertexClass("links");
    String edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    Vertex vrt = db.newVertex(clazz.getName());
    Vertex link = db.newVertex(linkClass.getName());
    link.save();
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt = db.bindToSession(vrt);
    vrt.getEdges(Direction.OUT, edgeClass).forEach(Entity::delete);
    vrt.save();
    Vertex link2 = db.newVertex(linkClass.getName());
    link2.save();
    vrt.addLightWeightEdge(link2, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt = db.load(vrt.getIdentity());
    Assert.assertEquals(
        link2.getIdentity(),
        vrt.getVertices(Direction.OUT, edgeClass).iterator().next().getIdentity());
    db.commit();
  }

  @Test
  public void maxConstraintOnFloatPropertyDuringTransaction() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "dbl", PropertyType.FLOAT).setMandatory(db, true).setMin(db, "-10");
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("dbl", -100.0);
    vertex.save();
    vertex.setProperty("dbl", 2.39);
    vertex.save();
    db.commit();
    db.begin();
    vertex = db.bindToSession(vertex);
    float actual = vertex.getProperty("dbl");
    Assert.assertEquals(2.39, actual, 0.01);
    db.commit();
  }

  @Test(expected = ValidationException.class)
  public void maxConstraintOnFloatPropertyOnTransaction() {
    SchemaClass clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "dbl", PropertyType.FLOAT).setMandatory(db, true).setMin(db, "-10");
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("dbl", -100.0);
    vertex.save();
    db.commit();
  }
}
