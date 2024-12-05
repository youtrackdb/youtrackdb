package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.exception.YTTooBigIndexKeyException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Test;

public class BigKeyIndexTest extends DBTestBase {

  @Test
  public void testBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
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

  @Test(expected = YTTooBigIndexKeyException.class)
  public void testTooBigKey() {
    YTClass cl = db.createClass("One");
    YTProperty prop = cl.createProperty(db, "two", YTType.STRING);
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
