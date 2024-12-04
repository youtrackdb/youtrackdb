package com.orientechnologies.orient.core.index;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.YTProperty;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class UniqueHashIndexForDate extends DBTestBase {

  @Test
  public void testSimpleUniqueDateIndex() throws ParseException {
    YTClass clazz = db.getMetadata().getSchema().createClass("test_edge");
    YTProperty prop = clazz.createProperty(db, "date", YTType.DATETIME);
    prop.createIndex(db, INDEX_TYPE.UNIQUE);
    YTDocument doc = new YTDocument("test_edge");
    doc.field("date", "2015-03-24 08:54:49");

    YTDocument doc1 = new YTDocument("test_edge");
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
