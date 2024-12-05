package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.ODirection;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import com.jetbrains.youtrack.db.internal.core.storage.YTRecordDuplicatedException;
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
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
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
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
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
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(ODirection.OUT, "Link");
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
    EntityImpl john = new EntityImpl("User");
    john.field("MailAddress", "john@doe.com");
    db.save(john);
    db.commit();

    db.begin();
    EntityImpl jane = new EntityImpl("User");
    jane.field("MailAddress", "jane@doe.com");
    EntityImpl id = jane;
    jane.save();
    db.save(jane);
    db.commit();

    try {
      db.begin();
      EntityImpl toUp = db.load(id.getIdentity());
      toUp.field("MailAddress", "john@doe.com");
      db.save(toUp);
      db.commit();
      Assert.fail("Expected record duplicate exception");
    } catch (YTRecordDuplicatedException ex) {
      // ignore
    }
    EntityImpl fromDb = db.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final YTSchema schema = db.getMetadata().getSchema();
    YTClass userClass = schema.createClass("User");
    userClass.createProperty(db, "MailAddress", YTType.STRING)
        .createIndex(db, YTClass.INDEX_TYPE.UNIQUE);

    db.begin();
    EntityImpl jane = new EntityImpl("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();
    db.commit();

    final YTRID rid = jane.getIdentity();

    reOpen("admin", "adminpwd");

    db.begin();
    EntityImpl joneJane = db.load(rid);

    joneJane.field("MailAddress", "john@doe.com");
    joneJane.field("@version", -1);

    joneJane.save();
    db.commit();

    reOpen("admin", "adminpwd");

    try {
      db.begin();
      EntityImpl toUp = new EntityImpl("User");
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
