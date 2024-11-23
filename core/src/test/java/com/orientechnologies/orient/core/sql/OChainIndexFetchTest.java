package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class OChainIndexFetchTest extends BaseMemoryDatabase {

  @Test
  public void testFetchChaninedIndex() {
    OClass baseClass = db.getMetadata().getSchema().createClass("BaseClass");
    OProperty propr = baseClass.createProperty(db, "ref", OType.LINK);

    OClass linkedClass = db.getMetadata().getSchema().createClass("LinkedClass");
    OProperty id = linkedClass.createProperty(db, "id", OType.STRING);
    id.createIndex(db, INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(db, linkedClass);
    propr.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    ODocument doc = new ODocument(linkedClass);
    doc.field("id", "referred");
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    ODocument doc1 = new ODocument(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);
    db.commit();

    OResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
