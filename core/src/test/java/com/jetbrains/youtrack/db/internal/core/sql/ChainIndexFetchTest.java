package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class ChainIndexFetchTest extends DbTestBase {

  @Test
  public void testFetchChaninedIndex() {
    SchemaClass baseClass = db.getMetadata().getSchema().createClass("BaseClass");
    SchemaProperty propr = baseClass.createProperty(db, "ref", PropertyType.LINK);

    SchemaClass linkedClass = db.getMetadata().getSchema().createClass("LinkedClass");
    SchemaProperty id = linkedClass.createProperty(db, "id", PropertyType.STRING);
    id.createIndex(db, INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(db, linkedClass);
    propr.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    EntityImpl doc = new EntityImpl(linkedClass);
    doc.field("id", "referred");
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    EntityImpl doc1 = new EntityImpl(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);
    db.commit();

    ResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
