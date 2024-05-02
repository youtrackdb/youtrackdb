package com.orientechnologies.orient.core.record.impl;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.exception.OValidationException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.OVertex;
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

}
