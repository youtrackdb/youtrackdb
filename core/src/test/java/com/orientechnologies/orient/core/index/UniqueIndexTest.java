package com.orientechnologies.orient.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.ODirection;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.storage.YTRecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UniqueIndexTest extends DBTestBase {

  @Test
  public void compositeIndexWithEdgesTestOne() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = YTVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, YTType.LINKBAG);

    entityClass.createProperty(db, "type", YTType.STRING);
    entityClass.createIndex(db, "typeLink", YTClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

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
    secondEntity = db.bindToSession(secondEntity);
    secondEntity.setProperty("type", "type1");
    secondEntity.save();
    try {
      db.commit();
      Assert.fail();
    } catch (YTRecordDuplicatedException e) {
      db.rollback();
    }
  }

  @Test
  public void compositeIndexWithEdgesTestTwo() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = YTVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, YTType.LINKBAG);

    entityClass.createProperty(db, "type", YTType.STRING);
    entityClass.createIndex(db, "typeLink", YTClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

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
  }

  @Test
  public void compositeIndexWithEdgesTestThree() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = YTVertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, YTType.LINKBAG);

    entityClass.createProperty(db, "type", YTType.STRING);
    entityClass.createIndex(db, "typeLink", YTClass.INDEX_TYPE.UNIQUE, "type", edgeOutPropertyName);

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
  }

  @Test()
  public void testUniqueOnUpdate() {
    final YTSchema schema = db.getMetadata().getSchema();
    YTClass userClass = schema.createClass("User");
    userClass.createProperty(db, "MailAddress", YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    db.begin();
    YTDocument john = new YTDocument("User");
    john.field("MailAddress", "john@doe.com");
    db.save(john);
    db.commit();

    db.begin();
    YTDocument jane = new YTDocument("User");
    jane.field("MailAddress", "jane@doe.com");
    YTDocument id = jane;
    jane.save();
    db.save(jane);
    db.commit();

    try {
      db.begin();
      YTDocument toUp = db.load(id.getIdentity());
      toUp.field("MailAddress", "john@doe.com");
      db.save(toUp);
      db.commit();
      Assert.fail("Expected record duplicate exception");
    } catch (YTRecordDuplicatedException ex) {
      // ignore
    }
    YTDocument fromDb = db.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final YTSchema schema = db.getMetadata().getSchema();
    YTClass userClass = schema.createClass("User");
    userClass.createProperty(db, "MailAddress", YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    db.begin();
    YTDocument jane = new YTDocument("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();
    db.commit();

    final YTRID rid = jane.getIdentity();

    reOpen("admin", "adminpwd");

    db.begin();
    YTDocument joneJane = db.load(rid);

    joneJane.field("MailAddress", "john@doe.com");
    joneJane.field("@version", -1);

    joneJane.save();
    db.commit();

    reOpen("admin", "adminpwd");

    try {
      db.begin();
      YTDocument toUp = new YTDocument("User");
      toUp.field("MailAddress", "john@doe.com");

      db.save(toUp);
      db.commit();

      Assert.fail("Expected record duplicate exception");
    } catch (YTRecordDuplicatedException ex) {
      // ignore
    }

    final YTResultSet result = db.query("select from User where MailAddress = 'john@doe.com'");
    Assert.assertEquals(result.stream().count(), 1);
  }
}
