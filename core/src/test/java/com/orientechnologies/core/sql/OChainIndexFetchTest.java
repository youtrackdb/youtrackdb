package com.orientechnologies.core.sql;

import static org.junit.Assert.assertEquals;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResultSet;
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
    YTEntityImpl doc = new YTEntityImpl(linkedClass);
    doc.field("id", "referred");
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    YTEntityImpl doc1 = new YTEntityImpl(baseClass);
    doc1.field("ref", doc);

    db.save(doc1);
    db.commit();

    YTResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
