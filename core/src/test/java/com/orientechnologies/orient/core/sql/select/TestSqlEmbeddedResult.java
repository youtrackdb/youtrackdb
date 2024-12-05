package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlEmbeddedResult extends DBTestBase {

  @Test
  public void testEmbeddedRusultTypeNotLink() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    YTEntityImpl doc1 = new YTEntityImpl();
    doc1.setProperty("format", 1);
    Set<YTEntityImpl> docs = new HashSet<YTEntityImpl>();
    docs.add(doc1);
    doc.setProperty("rel", docs);
    // doc
    db.save(doc);
    db.commit();

    List<YTEntity> res =
        db
            .query(
                "select $current as el " + " from (select expand(rel.include('format')) from Test)")
            .stream()
            .map(YTResult::toEntity)
            .collect(Collectors.toList());
    Assert.assertEquals(res.size(), 1);
    YTEntity ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof YTEntityImpl);

    res =
        db.query("select rel as el " + " from (select rel from Test)").stream()
            .map(YTResult::toEntity)
            .toList();

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof Set<?>);
  }
}
