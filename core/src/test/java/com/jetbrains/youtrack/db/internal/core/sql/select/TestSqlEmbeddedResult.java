package com.jetbrains.youtrack.db.internal.core.sql.select;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
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

    List<Entity> res =
        db
            .query(
                "select $current as el " + " from (select expand(rel.include('format')) from Test)")
            .stream()
            .map(result -> result.toEntity())
            .collect(Collectors.toList());
    Assert.assertEquals(res.size(), 1);
    Entity ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof EntityImpl);

    res =
        db.query("select rel as el " + " from (select rel from Test)").stream()
            .map(result -> result.toEntity())
            .toList();

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof Set<?>);
  }
}
