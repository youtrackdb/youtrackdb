package com.jetbrains.youtrack.db.internal.core.sql.select;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlEmbeddedResult extends DbTestBase {

  @Test
  public void testEmbeddedRusultTypeNotLink() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    EntityImpl doc = ((EntityImpl) db.newEntity("Test"));
    EntityImpl doc1 = ((EntityImpl) db.newEntity());
    doc1.setProperty("format", 1);
    Set<EntityImpl> docs = new HashSet<EntityImpl>();
    docs.add(doc1);
    doc.setProperty("rel", docs);
    // doc
    db.save(doc);
    db.commit();

    var res =
        db
            .query(
                "select $current as el " + " from (select expand(rel.include('format')) from Test)")
            .toList();
    Assert.assertEquals(1, res.size());
    var ele = res.getFirst();
    Assert.assertTrue(ele.getProperty("el") instanceof EntityImpl);

    res =
        db.query("select rel as el " + " from (select rel from Test)").stream()
            .toList();

    Assert.assertEquals(1, res.size());
    ele = res.getFirst();
    Assert.assertTrue(ele.getProperty("el") instanceof Set<?>);
  }
}
