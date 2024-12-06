package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.junit.Assert;
import org.junit.Test;

public class NestedInsertTest extends DbTestBase {

  @Test
  public void testEmbeddedValueDate() {
    Schema schm = db.getMetadata().getSchema();
    schm.createClass("myClass");

    db.begin();
    ResultSet result =
        db.command(
            "insert into myClass (name,meta) values"
                + " (\"claudio\",{\"@type\":\"d\",\"country\":\"italy\","
                + " \"date\":\"2013-01-01\",\"@fieldTypes\":\"date=a\"}) return @this");
    db.commit();

    final EntityImpl res = ((Identifiable) result.next().getProperty("@this")).getRecord();
    final EntityImpl embedded = res.field("meta");
    Assert.assertNotNull(embedded);

    Assert.assertEquals(embedded.fields(), 2);
    Assert.assertEquals(embedded.field("country"), "italy");
    Assert.assertEquals(embedded.field("date").getClass(), java.util.Date.class);
  }

  @Test
  public void testLinkedNested() {
    Schema schm = db.getMetadata().getSchema();
    SchemaClass cl = schm.createClass("myClass");
    SchemaClass linked = schm.createClass("Linked");
    cl.createProperty(db, "some", PropertyType.LINK, linked);

    db.begin();
    ResultSet result =
        db.command(
            "insert into myClass set some ={\"@type\":\"d\",\"@class\":\"Linked\",\"name\":\"a"
                + " name\"} return @this");
    db.commit();

    final EntityImpl res = ((Identifiable) result.next().getProperty("@this")).getRecord();
    final EntityImpl ln = res.field("some");
    Assert.assertNotNull(ln);
    Assert.assertTrue(ln.getIdentity().isPersistent());
    Assert.assertEquals(ln.fields(), 1);
    Assert.assertEquals(ln.field("name"), "a name");
  }
}
