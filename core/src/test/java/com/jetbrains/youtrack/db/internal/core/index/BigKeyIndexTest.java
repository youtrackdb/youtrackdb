package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.TooBigIndexKeyException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class BigKeyIndexTest extends DbTestBase {

  @Test
  public void testBigKey() {
    SchemaClass cl = db.createClass("One");
    SchemaProperty prop = cl.createProperty(db, "two", PropertyType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    for (int i = 0; i < 100; i++) {
      db.begin();
      EntityImpl doc = db.newInstance("One");
      StringBuilder bigValue = new StringBuilder(i % 1000 + "one10000");
      for (int z = 0; z < 218; z++) {
        bigValue.append("one").append(z);
      }
      doc.setProperty("two", bigValue.toString());

      db.save(doc);
      db.commit();
    }
  }

  @Test(expected = TooBigIndexKeyException.class)
  public void testTooBigKey() {
    SchemaClass cl = db.createClass("One");
    SchemaProperty prop = cl.createProperty(db, "two", PropertyType.STRING);
    prop.createIndex(db, INDEX_TYPE.NOTUNIQUE);

    db.begin();
    EntityImpl doc = db.newInstance("One");
    StringBuilder bigValue = new StringBuilder();
    for (int z = 0; z < 5000; z++) {
      bigValue.append("one").append(z);
    }
    doc.setProperty("two", bigValue.toString());
    db.save(doc);
    db.commit();
  }
}
