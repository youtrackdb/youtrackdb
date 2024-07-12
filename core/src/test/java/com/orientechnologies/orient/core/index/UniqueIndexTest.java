package com.orientechnologies.orient.core.index;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.storage.ORecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by tglman on 01/02/16.
 */
public class UniqueIndexTest extends BaseMemoryDatabase {

  @Test
  public void compositeIndexWithEdgesTestOne() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(edgeOutPropertyName, OType.LINKBAG);

    entityClass.createProperty("type", OType.STRING);
    entityClass.createIndex("typeLink", OClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

    db.begin();
    var firstEntity = db.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = db.newVertex(entityClass);
    secondEntity.setProperty("type", "type2");

    var thirdEntity = db.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);
    secondEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();

    db.commit();

    db.begin();
    secondEntity.setProperty("type", "type1");
    secondEntity.save();
    try {
      db.commit();
      Assert.fail();
    } catch (ORecordDuplicatedException e) {
      db.rollback();
    }
  }

  @Test
  public void compositeIndexWithEdgesTestTwo() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(edgeOutPropertyName, OType.LINKBAG);

    entityClass.createProperty("type", OType.STRING);
    entityClass.createIndex("typeLink", OClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

    db.begin();
    var firstEntity = db.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = db.newVertex(entityClass);
    secondEntity.setProperty("type", "type2");

    var thirdEntity = db.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);
    secondEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();
    db.commit();

    db.begin();
    secondEntity.setProperty("type", "type1");
    secondEntity.deleteEdge(thirdEntity, linkClass);
    secondEntity.save();
    db.commit();
  }

  @Test
  public void compositeIndexWithEdgesTestThree() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = OVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(edgeOutPropertyName, OType.LINKBAG);

    entityClass.createProperty("type", OType.STRING);
    entityClass.createIndex("typeLink", OClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

    db.begin();
    var firstEntity = db.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = db.newVertex(entityClass);
    secondEntity.setProperty("type", "type1");

    var thirdEntity = db.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();
    db.commit();

    db.begin();
    firstEntity.deleteEdge(thirdEntity, linkClass);
    secondEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();
    db.commit();
  }

  @Test()
  public void testUniqueOnUpdate() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass userClass = schema.createClass("User");
    userClass.createProperty("MailAddress", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);

    ODocument john = new ODocument("User");
    john.field("MailAddress", "john@doe.com");
    db.save(john);

    ODocument jane = new ODocument("User");
    jane.field("MailAddress", "jane@doe.com");
    ODocument id = jane.save();
    db.save(jane);

    try {
      ODocument toUp = db.load(id.getIdentity());
      toUp.field("MailAddress", "john@doe.com");
      db.save(toUp);
      Assert.fail("Expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {
      //ignore
    }
    ODocument fromDb = db.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final OSchema schema = db.getMetadata().getSchema();
    OClass userClass = schema.createClass("User");
    userClass.createProperty("MailAddress", OType.STRING).createIndex(OClass.INDEX_TYPE.UNIQUE);

    ODocument jane = new ODocument("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();

    final ORID rid = jane.getIdentity();

    reOpen("admin", "adminpwd");

    ODocument joneJane = db.load(rid);

    joneJane.field("MailAddress", "john@doe.com");
    joneJane.field("@version", -1);

    joneJane.save();

    reOpen("admin", "adminpwd");

    try {
      ODocument toUp = new ODocument("User");
      toUp.field("MailAddress", "john@doe.com");

      db.save(toUp);
      Assert.fail("Expected record duplicate exception");
    } catch (ORecordDuplicatedException ex) {
      //ignore
    }

    final OResultSet result = db.query("select from User where MailAddress = 'john@doe.com'");
    Assert.assertEquals(result.stream().count(), 1);
  }
}
