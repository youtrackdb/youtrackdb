package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTSchema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class NestedInsertTest extends DBTestBase {

  @Test
  public void testEmbeddedValueDate() {
    YTSchema schm = db.getMetadata().getSchema();
    schm.createClass("myClass");

    db.begin();
    YTResultSet result =
        db.command(
            "insert into myClass (name,meta) values"
                + " (\"claudio\",{\"@type\":\"d\",\"country\":\"italy\","
                + " \"date\":\"2013-01-01\",\"@fieldTypes\":\"date=a\"}) return @this");
    db.commit();

    final EntityImpl res = ((YTIdentifiable) result.next().getProperty("@this")).getRecord();
    final EntityImpl embedded = res.field("meta");
    Assert.assertNotNull(embedded);

    Assert.assertEquals(embedded.fields(), 2);
    Assert.assertEquals(embedded.field("country"), "italy");
    Assert.assertEquals(embedded.field("date").getClass(), java.util.Date.class);
  }

  @Test
  public void testLinkedNested() {
    YTSchema schm = db.getMetadata().getSchema();
    YTClass cl = schm.createClass("myClass");
    YTClass linked = schm.createClass("Linked");
    cl.createProperty(db, "some", YTType.LINK, linked);

    db.begin();
    YTResultSet result =
        db.command(
            "insert into myClass set some ={\"@type\":\"d\",\"@class\":\"Linked\",\"name\":\"a"
                + " name\"} return @this");
    db.commit();

    final EntityImpl res = ((YTIdentifiable) result.next().getProperty("@this")).getRecord();
    final EntityImpl ln = res.field("some");
    Assert.assertNotNull(ln);
    Assert.assertTrue(ln.getIdentity().isPersistent());
    Assert.assertEquals(ln.fields(), 1);
    Assert.assertEquals(ln.field("name"), "a name");
  }
}
