package com.jetbrains.youtrack.db.internal.core.index;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.text.ParseException;
import org.junit.Assert;
import org.junit.Test;

public class UniqueHashIndexForDate extends DbTestBase {

  @Test
  public void testSimpleUniqueDateIndex() throws ParseException {
    var clazz = db.getMetadata().getSchema().createClass("test_edge");
    var prop = clazz.createProperty(db, "date", PropertyType.DATETIME);
    prop.createIndex(db, INDEX_TYPE.UNIQUE);
    var doc = (EntityImpl) db.newEntity("test_edge");
    doc.field("date", "2015-03-24 08:54:49");

    var doc1 = (EntityImpl) db.newEntity("test_edge");
    doc1.field("date", "2015-03-24 08:54:49");

    db.save(doc);
    try {
      db.begin();
      db.save(doc1);
      doc1.field("date", "2015-03-24 08:54:49");
      db.save(doc1);
      db.commit();
      Assert.fail("expected exception for duplicate ");
    } catch (BaseException e) {

    }
  }
}
