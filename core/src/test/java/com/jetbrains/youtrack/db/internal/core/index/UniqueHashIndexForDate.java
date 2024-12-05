package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class UniqueHashIndexForDate extends DBTestBase {

  @Test
  public void testSimpleUniqueDateIndex() throws ParseException {
    YTClass clazz = db.getMetadata().getSchema().createClass("test_edge");
    YTProperty prop = clazz.createProperty(db, "date", YTType.DATETIME);
    prop.createIndex(db, INDEX_TYPE.UNIQUE);
    EntityImpl doc = new EntityImpl("test_edge");
    doc.field("date", "2015-03-24 08:54:49");

    EntityImpl doc1 = new EntityImpl("test_edge");
    doc1.field("date", "2015-03-24 08:54:49");

    db.save(doc);
    try {
      db.begin();
      db.save(doc1);
      doc1.field("date", "2015-03-24 08:54:49");
      db.save(doc1);
      db.commit();
      Assert.fail("expected exception for duplicate ");
    } catch (YTException e) {

    }
  }
}
