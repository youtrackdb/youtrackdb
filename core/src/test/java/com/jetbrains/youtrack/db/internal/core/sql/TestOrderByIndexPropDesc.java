package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class TestOrderByIndexPropDesc extends DBTestBase {

  private static final String DOCUMENT_CLASS_NAME = "MyDocument";
  private static final String PROP_INDEXED_STRING = "dateProperty";

  public void beforeTest() throws Exception {
    super.beforeTest();
    YTClass oclass = db.getMetadata().getSchema().createClass(DOCUMENT_CLASS_NAME);
    oclass.createProperty(db, PROP_INDEXED_STRING, YTType.INTEGER);
    oclass.createIndex(db, "index", INDEX_TYPE.NOTUNIQUE, PROP_INDEXED_STRING);
  }

  @Test
  public void worksFor1000() {
    test(1000);
  }

  @Test
  public void worksFor10000() {
    test(50000);
  }

  private void test(int count) {
    EntityImpl doc;
    for (int i = 0; i < count; i++) {
      db.begin();
      doc = db.newInstance();
      doc.setClassName(DOCUMENT_CLASS_NAME);
      doc.field(PROP_INDEXED_STRING, i);
      db.save(doc);
      db.commit();
    }

    YTResultSet result =
        db.query(
            "select from " + DOCUMENT_CLASS_NAME + " order by " + PROP_INDEXED_STRING + " desc");

    Assert.assertEquals(count, result.stream().count());
  }
}
