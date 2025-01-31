package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class ChainIndexFetchTest extends DbTestBase {

  @Test
  public void testFetchChaninedIndex() {
    var baseClass = db.getMetadata().getSchema().createClass("BaseClass");
    var propr = baseClass.createProperty(db, "ref", PropertyType.LINK);

    var linkedClass = db.getMetadata().getSchema().createClass("LinkedClass");
    var id = linkedClass.createProperty(db, "id", PropertyType.STRING);
    id.createIndex(db, INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(db, linkedClass);
    propr.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    var doc = (EntityImpl) db.newEntity(linkedClass);
    doc.field("id", "referred");
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    var doc1 = (EntityImpl) db.newEntity(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);
    db.commit();

    var res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
