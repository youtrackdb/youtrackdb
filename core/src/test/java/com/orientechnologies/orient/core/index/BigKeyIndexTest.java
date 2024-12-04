package com.orientechnologies.orient.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Test;

public class BigKeyIndexTest extends DBTestBase {

  @Test
  public void testBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      db.begin();
      YTDocument doc = db.newInstance("One");
      StringBuilder bigValue = new StringBuilder(i % 1000 + "one10000");
      for (int z = 0; z < 218; z++) {
        bigValue.append("one").append(z);
      }
      doc.setProperty("two", bigValue.toString());

      db.save(doc);
      db.commit();
    }
  }

  @Test(expected = OTooBigIndexKeyException.class)
  public void testTooBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    YTDocument doc = db.newInstance("One");
    StringBuilder bigValue = new StringBuilder();
    for (int z = 0; z < 5000; z++) {
      bigValue.append("one").append(z);
    }
    doc.setProperty("two", bigValue.toString());
    db.save(doc);
    db.commit();
  }
}
