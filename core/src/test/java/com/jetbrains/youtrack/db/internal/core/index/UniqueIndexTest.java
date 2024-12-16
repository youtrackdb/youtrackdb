package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.record.Direction;
import com.jetbrains.youtrack.db.api.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.exception.RecordDuplicatedException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class UniqueIndexTest extends DbTestBase {

  @Test
  public void compositeIndexWithEdgesTestOne() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(db, "type", PropertyType.STRING);
    entityClass.createIndex(db, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

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
    } catch (RecordDuplicatedException e) {
      db.rollback();
    }
  }

  @Test
  public void compositeIndexWithEdgesTestTwo() {
    var linkClass = db.createLightweightEdgeClass("Link");

    var entityClass = db.createVertexClass("Entity");
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(db, "type", PropertyType.STRING);
    entityClass.createIndex(db, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

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
    var edgeOutPropertyName = Vertex.getEdgeLinkFieldName(Direction.OUT, "Link");
    entityClass.createProperty(db, edgeOutPropertyName, PropertyType.LINKBAG);

    entityClass.createProperty(db, "type", PropertyType.STRING);
    entityClass.createIndex(db, "typeLink", SchemaClass.INDEX_TYPE.UNIQUE, "type",
        edgeOutPropertyName);

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
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass userClass = schema.createClass("User");
    userClass.createProperty(db, "MailAddress", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);

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
    } catch (RecordDuplicatedException ex) {
      // ignore
    }
    EntityImpl fromDb = db.load(id.getIdentity());
    Assert.assertEquals(fromDb.field("MailAddress"), "jane@doe.com");
  }

  @Test
  public void testUniqueOnUpdateNegativeVersion() {
    final Schema schema = db.getMetadata().getSchema();
    SchemaClass userClass = schema.createClass("User");
    userClass.createProperty(db, "MailAddress", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);

    db.begin();
    EntityImpl jane = new EntityImpl("User");
    jane.field("MailAddress", "jane@doe.com");
    jane.save();
    db.commit();

    final RID rid = jane.getIdentity();

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
    } catch (RecordDuplicatedException ex) {
      // ignore
    }

    final ResultSet result = db.query("select from User where MailAddress = 'john@doe.com'");
    Assert.assertEquals(result.stream().count(), 1);
  }
}
