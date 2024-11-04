package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODocumentTransactionalValidationTest extends BaseMemoryInternalDatabase {

  @Test(expected = OValidationException.class)
  public void simpleConstraintShouldBeCheckedOnCommitFalseTest() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("int", OType.INTEGER).setMandatory(true);
    db.begin().activateOnCurrentThread();
    var vertex = db.newVertex(clazz.getName());
    vertex.save();
    db.commit();
  }

  @Test()
  public void simpleConstraintShouldBeCheckedOnCommitTrueTest() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("int", OType.INTEGER).setMandatory(true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("int", 11);
    vertex.save();
    db.commit();
    db.begin();
    db.begin();
    Integer value = vertex.getProperty("int");
    Assert.assertEquals((Integer) 11, value);
  }

  @Test()
  public void simpleConstraintShouldBeCheckedOnCommitWithTypeConvert() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("int", OType.INTEGER).setMandatory(true);
    db.begin();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("int", "11");
    vertex.save();
    db.commit();
    db.begin();
    Integer value = vertex.getProperty("int");
    Assert.assertEquals((Integer) 11, value);
  }

  @Test
  public void stringRegexpPatternValidationCheck() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("str", OType.STRING).setMandatory(true).setRegexp("aba.*");
    OVertex vertex;
    db.begin();
    vertex = db.newVertex(clazz.getName());
    vertex.setProperty("str", "first");
    vertex.setProperty("str", "second");
    vertex.save();
    vertex.setProperty("str", "abacorrect");
    db.commit();
    Assert.assertEquals("abacorrect", vertex.getProperty("str"));
  }

  @Test(expected = OValidationException.class)
  public void stringRegexpPatternValidationCheckFails() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("str", OType.STRING).setMandatory(true).setRegexp("aba.*");
    OVertex vertex;
    db.begin();
    vertex = db.newVertex(clazz.getName());
    vertex.setProperty("str", "first");
    vertex.save();
    db.commit();
  }

  @Test(expected = OValidationException.class)
  public void requiredLinkBagNegativeTest() {
    OClass edgeClass = db.createEdgeClass("lst");
    OClass clazz = db.createVertexClass("Validation");
    OClass linkClass = db.createVertexClass("links");
    String edgePropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass.getName());
    clazz.createProperty(edgePropertyName, OType.LINKBAG, linkClass).setMandatory(true);
    db.begin();
    db.newVertex(clazz.getName()).save();
    db.commit();
  }

  @Test
  public void requiredLinkBagPositiveTest() {
    OClass edgeClass = db.createLightweightEdgeClass("lst");
    OClass clazz = db.createVertexClass("Validation");
    OClass linkClass = db.createVertexClass("links");
    String edgePropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass.getName());
    clazz.createProperty(edgePropertyName, OType.LINKBAG, linkClass).setMandatory(true);
    db.begin();
    OVertex vrt = db.newVertex(clazz.getName());
    OVertex link = db.newVertex(linkClass.getName());
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
  }

  @Test(expected = OValidationException.class)
  public void requiredLinkBagFailsIfBecomesEmpty() {
    OClass edgeClass = db.createEdgeClass("lst");
    OClass clazz = db.createVertexClass("Validation");
    OClass linkClass = db.createVertexClass("links");
    String edgePropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass.getName());
    clazz.createProperty(edgePropertyName, OType.LINKBAG, linkClass).setMandatory(true).setMin("1");
    db.begin();
    OVertex vrt = db.newVertex(clazz.getName());
    OVertex link = db.newVertex(linkClass.getName());
    vrt.addEdge(link, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt.getEdges(ODirection.OUT, edgeClass).forEach(OElement::delete);
    vrt.deleteEdge(link, edgeClass);
    vrt.save();
    db.commit();
  }

  @Test(expected = OValidationException.class)
  public void requiredArrayFailsIfBecomesEmpty() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("arr", OType.EMBEDDEDLIST).setMandatory(true).setMin("1");
    db.begin();
    OVertex vrt = db.newVertex(clazz.getName());
    vrt.setProperty("arr", Arrays.asList(1, 2, 3));
    vrt.save();
    db.commit();
    db.begin();
    List<Integer> arr = vrt.getProperty("arr");
    arr.clear();
    vrt.save();
    db.commit();
  }

  @Test
  public void requiredLinkBagCanBeEmptyDuringTransaction() {
    OClass edgeClass = db.createLightweightEdgeClass("lst");
    OClass clazz = db.createVertexClass("Validation");
    OClass linkClass = db.createVertexClass("links");
    String edgePropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, edgeClass.getName());
    clazz.createProperty(edgePropertyName, OType.LINKBAG, linkClass).setMandatory(true);
    db.begin();
    OVertex vrt = db.newVertex(clazz.getName());
    OVertex link = db.newVertex(linkClass.getName());
    link.save();
    vrt.addLightWeightEdge(link, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt.getEdges(ODirection.OUT, edgeClass).forEach(OElement::delete);
    vrt.save();
    OVertex link2 = db.newVertex(linkClass.getName());
    link2.save();
    vrt.addLightWeightEdge(link2, edgeClass);
    vrt.save();
    db.commit();
    db.begin();
    vrt = db.load(vrt.getIdentity());
    Assert.assertEquals(
        link2.getIdentity(),
        vrt.getVertices(ODirection.OUT, edgeClass).iterator().next().getIdentity());
    db.commit();
  }

  @Test
  public void maxConstraintOnFloatPropertyDuringTransaction() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("dbl", OType.FLOAT).setMandatory(true).setMin("-10");
    db.begin().activateOnCurrentThread();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("dbl", -100.0);
    vertex.save();
    vertex.setProperty("dbl", 2.39);
    vertex.save();
    db.commit();
    db.begin();
    float actual = vertex.getProperty("dbl");
    Assert.assertEquals(2.39, actual, 0.01);
    db.commit();
  }

  @Test(expected = OValidationException.class)
  public void maxConstraintOnFloatPropertyOnTransaction() {
    OClass clazz = db.createVertexClass("Validation");
    clazz.createProperty("dbl", OType.FLOAT).setMandatory(true).setMin("-10");
    db.begin().activateOnCurrentThread();
    var vertex = db.newVertex(clazz.getName());
    vertex.setProperty("dbl", -100.0);
    vertex.save();
    db.commit();
  }
}
