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
    var baseClass = session.getMetadata().getSchema().createClass("BaseClass");
    var propr = baseClass.createProperty(session, "ref", PropertyType.LINK);

    var linkedClass = session.getMetadata().getSchema().createClass("LinkedClass");
    var id = linkedClass.createProperty(session, "id", PropertyType.STRING);
    id.createIndex(session, INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(session, linkedClass);
    propr.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    session.begin();
    var doc = (EntityImpl) session.newEntity(linkedClass);
    doc.field("id", "referred");
    session.save(doc);
    session.commit();

    session.begin();

    doc = session.bindToSession(doc);
    var doc1 = (EntityImpl) session.newEntity(baseClass);
    doc1.field("ref", doc);

    session.save(doc1);
    session.commit();

    var res = session.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
