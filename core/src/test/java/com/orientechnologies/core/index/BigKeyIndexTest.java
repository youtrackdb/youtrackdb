package com.orientechnologies.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.exception.YTTooBigIndexKeyException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Test;

public class BigKeyIndexTest extends DBTestBase {

  @Test
  public void testBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      db.begin();
      YTEntityImpl doc = db.newInstance("One");
      StringBuilder bigValue = new StringBuilder(i % 1000 + "one10000");
      for (int z = 0; z < 218; z++) {
        bigValue.append("one").append(z);
      }
      doc.setProperty("two", bigValue.toString());

      db.save(doc);
      db.commit();
    }
  }

  @Test(expected = YTTooBigIndexKeyException.class)
  public void testTooBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    YTEntityImpl doc = db.newInstance("One");
    StringBuilder bigValue = new StringBuilder();
    for (int z = 0; z < 5000; z++) {
      bigValue.append("one").append(z);
    }
    doc.setProperty("two", bigValue.toString());
    db.save(doc);
    db.commit();
  }
}
