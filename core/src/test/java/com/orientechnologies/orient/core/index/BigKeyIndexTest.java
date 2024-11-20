package com.orientechnologies.orient.core.index;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.exception.OTooBigIndexKeyException;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.record.impl.ODocument;
import org.junit.Test;

public class BigKeyIndexTest extends BaseMemoryDatabase {

  @Test
  public void testBigKey() {
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      db.begin();
      ODocument doc = db.newInstance("One");
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
    OClass cl = db.createClass("One");
    OProperty prop = cl.createProperty("two", OType.STRING);
    prop.createIndex(INDEX_TYPE.NOTUNIQUE);

    db.begin();
    ODocument doc = db.newInstance("One");
    StringBuilder bigValue = new StringBuilder();
    for (int z = 0; z < 5000; z++) {
      bigValue.append("one").append(z);
    }
    doc.setProperty("two", bigValue.toString());
    db.save(doc);
    db.commit();
  }
}
