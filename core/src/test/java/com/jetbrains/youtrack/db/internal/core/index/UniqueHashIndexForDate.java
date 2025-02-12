package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class UniqueHashIndexForDate extends DbTestBase {

  @Test
  public void testSimpleUniqueDateIndex() throws ParseException {
    var clazz = session.getMetadata().getSchema().createClass("test_edge");
    var prop = clazz.createProperty(session, "date", PropertyType.DATETIME);
    prop.createIndex(session, INDEX_TYPE.UNIQUE);
    var doc = (EntityImpl) session.newEntity("test_edge");
    doc.field("date", "2015-03-24 08:54:49");

    var doc1 = (EntityImpl) session.newEntity("test_edge");
    doc1.field("date", "2015-03-24 08:54:49");

    session.save(doc);
    try {
      session.begin();
      session.save(doc1);
      doc1.field("date", "2015-03-24 08:54:49");
      session.save(doc1);
      session.commit();
      Assert.fail("expected exception for duplicate ");
    } catch (BaseException e) {

    }
  }
}
