package com.orientechnologies.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class UniqueHashIndexForDate extends DBTestBase {

  @Test
  public void testSimpleUniqueDateIndex() throws ParseException {
    YTClass clazz = db.getMetadata().getSchema().createClass("test_edge");
    YTProperty prop = clazz.createProperty(db, "date", YTType.DATETIME);
    prop.createIndex(db, INDEX_TYPE.UNIQUE);
    YTEntityImpl doc = new YTEntityImpl("test_edge");
    doc.field("date", "2015-03-24 08:54:49");

    YTEntityImpl doc1 = new YTEntityImpl("test_edge");
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
