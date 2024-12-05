package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.impl.YTDocument;
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
    YTDocument doc = new YTDocument("Test");
    YTDocument doc1 = new YTDocument();
    doc1.setProperty("format", 1);
    Set<YTDocument> docs = new HashSet<YTDocument>();
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
    Assert.assertTrue(ele.getProperty("el") instanceof YTDocument);

    res =
        db.query("select rel as el " + " from (select rel from Test)").stream()
            .map(YTResult::toEntity)
            .toList();

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof Set<?>);
  }
}
