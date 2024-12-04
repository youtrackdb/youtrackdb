package com.orientechnologies.orient.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Test;

public class OChainIndexFetchTest extends DBTestBase {

  @Test
  public void testFetchChaninedIndex() {
    YTClass baseClass = db.getMetadata().getSchema().createClass("BaseClass");
    YTProperty propr = baseClass.createProperty(db, "ref", YTType.LINK);

    YTClass linkedClass = db.getMetadata().getSchema().createClass("LinkedClass");
    YTProperty id = linkedClass.createProperty(db, "id", YTType.STRING);
    id.createIndex(db, INDEX_TYPE.UNIQUE);

    propr.setLinkedClass(db, linkedClass);
    propr.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    YTDocument doc = new YTDocument(linkedClass);
    doc.field("id", "referred");
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    YTDocument doc1 = new YTDocument(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);
    db.commit();

    OResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
