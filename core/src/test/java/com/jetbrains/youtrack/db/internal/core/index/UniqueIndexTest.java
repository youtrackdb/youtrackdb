package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UniqueIndexTest extends DbTestBase {

  @Test
  public void compositeIndexWithEdgesTestOne() {
    var linkClass = session.createLightweightEdgeClass("Link");

    var entityClass = session.createVertexClass("Entity");
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(session, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(session, "type", PropertyType.STRING);
    entityClass.createIndex(session, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

    session.begin();
    var firstEntity = session.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = session.newVertex(entityClass);
    secondEntity.setProperty("type", "type2");

    var thirdEntity = session.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);
    secondEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();

    session.commit();

    session.begin();
    secondEntity = session.bindToSession(secondEntity);
    secondEntity.setProperty("type", "type1");
    secondEntity.save();
    try {
      session.commit();
      Assert.fail();
    } catch (RecordDuplicatedException e) {
      session.rollback();
    }
  }

  @Test
  public void compositeIndexWithEdgesTestTwo() {
    var linkClass = session.createLightweightEdgeClass("Link");

    var entityClass = session.createVertexClass("Entity");
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(session, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(session, "type", PropertyType.STRING);
    entityClass.createIndex(session, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

    session.begin();
    var firstEntity = session.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = session.newVertex(entityClass);
    secondEntity.setProperty("type", "type2");

    var thirdEntity = session.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);
    secondEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();
    session.commit();
  }

  @Test
  public void compositeIndexWithEdgesTestThree() {
    var linkClass = session.createLightweightEdgeClass("Link");

    var entityClass = session.createVertexClass("Entity");
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(session, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(session, "type", PropertyType.STRING);
    entityClass.createIndex(session, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

    session.begin();
    var firstEntity = session.newVertex(entityClass);
    firstEntity.setProperty("type", "type1");

    var secondEntity = session.newVertex(entityClass);
    secondEntity.setProperty("type", "type1");

    var thirdEntity = session.newVertex(entityClass);
    thirdEntity.setProperty("type", "type3");

    firstEntity.addLightWeightEdge(thirdEntity, linkClass);

    firstEntity.save();
    secondEntity.save();
    session.commit();
  }

  @Test()
  public void testUniqueOnUpdate() {
    final Schema schema = session.getMetadata().getSchema();
    var userClass = schema.createClass("User");
    userClass.createProperty(session, "MailAddress", PropertyType.STRING)
        .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);

    session.begin();
    var john = (EntityImpl) session.newEntity("User");
    john.field("MailAddress", "john@doe.com");
    session.save(john);
    session.commit();

    session.begin();
    var jane = (EntityImpl) session.newEntity("User");
    jane.field("MailAddress", "jane@doe.com");
    var id = jane;
    jane.save();
    session.save(jane);
    session.commit();

    try {
      session.begin();
      EntityImpl toUp = session.load(id.getIdentity());
      toUp.field("MailAddress", "john@doe.com");
      session.save(toUp);
      session.commit();
      Assert.fail("Expected record duplicate exception");
    } catch (RecordDuplicatedException ex) {
      // ignore
    }
    EntityImpl fromDb = session.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final Schema schema = session.getMetadata().getSchema();
    var userClass = schema.createClass("User");
    userClass.createProperty(session, "MailAddress", PropertyType.STRING)
        .createIndex(session, SchemaClass.INDEX_TYPE.UNIQUE);

    session.begin();
    var jane = (EntityImpl) session.newEntity("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();
    session.commit();

    final RID rid = jane.getIdentity();

    reOpen("admin", "adminpwd");

    session.begin();
    var joneJane = session.loadEntity(rid);

    joneJane.setProperty("MailAddress", "john@doe.com");
    session.commit();

    reOpen("admin", "adminpwd");

    try {
      session.begin();
      var toUp = (EntityImpl) session.newEntity("User");
      toUp.field("MailAddress", "john@doe.com");

      session.save(toUp);
      session.commit();

      Assert.fail("Expected record duplicate exception");
    } catch (RecordDuplicatedException ex) {
      // ignore
    }

    final var result = session.query("select from User where MailAddress = 'john@doe.com'");
    Assert.assertEquals(result.stream().count(), 1);
  }
}
