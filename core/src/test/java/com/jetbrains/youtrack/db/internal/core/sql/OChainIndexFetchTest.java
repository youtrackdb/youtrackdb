package com.jetbrains.youtrack.db.internal.core.sql;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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

    YTResultSet res = db.query(" select from BaseClass where ref.id ='wrong_referred' ");

    assertEquals(0, res.stream().count());
  }
}
