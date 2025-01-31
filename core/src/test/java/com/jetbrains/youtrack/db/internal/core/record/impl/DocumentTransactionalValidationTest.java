package com.jetbrains.youtrack.db.internal.core.record.impl;

import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.api.exception.ValidationException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Vertex;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class DocumentTransactionalValidationTest extends BaseMemoryInternalDatabase {

  @Test(expected = ValidationException.class)
  public void simpleConstraintShouldBeCheckedOnCommitFalseTest() {
    var clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "int", PropertyType.INTEGER).setMandatory(db, true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.save();
    db.commit();
  }

  @Test()
  public void simpleConstraintShouldBeCheckedOnCommitTrueTest() {
    var clazz = db.createVertexClass("Validation");
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
    var clazz = db.createVertexClass("Validation");
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
    var clazz = db.createVertexClass("Validation");
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
    var clazz = db.createVertexClass("Validation");
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
    var edgeClass = db.createEdgeClass("lst");
    var clazz = db.createVertexClass("Validation");
    var linkClass = db.createVertexClass("links");
    var edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    db.newVertex(clazz.getName()).save();
    db.commit();
  }

  @Test
  public void requiredLinkBagPositiveTest() {
    var edgeClass = db.createLightweightEdgeClass("lst");
    var clazz = db.createVertexClass("Validation");
    var linkClass = db.createVertexClass("links");
    var edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    var vrt = db.newVertex(clazz.getName());
    var link = db.newVertex(linkClass.getName());
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
  }

  @Test(expected = ValidationException.class)
  public void requiredLinkBagFailsIfBecomesEmpty() {
    var edgeClass = db.createEdgeClass("lst");
    var clazz = db.createVertexClass("Validation");
    var linkClass = db.createVertexClass("links");
    var edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true)
        .setMin(db, "1");
    db.begin();
    var vrt = db.newVertex(clazz.getName());
    var link = db.newVertex(linkClass.getName());
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
    var clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "arr", PropertyType.EMBEDDEDLIST).setMandatory(db, true)
        .setMin(db, "1");
    db.begin();
    var vrt = db.newVertex(clazz.getName());
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
    var edgeClass = db.createLightweightEdgeClass("lst");
    var clazz = db.createVertexClass("Validation");
    var linkClass = db.createVertexClass("links");
    var edgePropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, edgeClass.getName());
    clazz.createProperty(db, edgePropertyName, PropertyType.LINKBAG, linkClass)
        .setMandatory(db, true);
    db.begin();
    var vrt = db.newVertex(clazz.getName());
    var link = db.newVertex(linkClass.getName());
    link.save();
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt = db.bindToSession(vrt);
    vrt.getEdges(Direction.OUT, edgeClass).forEach(Entity::delete);
    vrt.save();
    var link2 = db.newVertex(linkClass.getName());
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
    var clazz = db.createVertexClass("Validation");
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
    var clazz = db.createVertexClass("Validation");
    clazz.createProperty(db, "dbl", PropertyType.FLOAT).setMandatory(db, true).setMin(db, "-10");
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("dbl", -100.0);
    vertex.save();
    db.commit();
  }
}
