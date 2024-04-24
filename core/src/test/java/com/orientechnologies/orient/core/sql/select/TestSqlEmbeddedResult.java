package com.orientechnologies.orient.core.sql.select;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;

public class TestSqlEmbeddedResult extends BaseMemoryDatabase {

  @Test
  public void testEmbeddedRusultTypeNotLink() {
    db.getMetadata().getSchema().createClass("Test");
    ODocument doc = new ODocument("Test");
    ODocument doc1 = new ODocument();
    doc1.setProperty("format", 1);
    Set<ODocument> docs = new HashSet<ODocument>();
    docs.add(doc1);
    doc.setProperty("rel", docs);
    // doc
    db.save(doc);

    List<OElement> res =
        db
            .query(
                "select $current as el " + " from (select expand(rel.include('format')) from Test)")
            .stream()
            .map(OResult::toElement)
            .collect(Collectors.toList());
    Assert.assertEquals(res.size(), 1);
    OElement ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof ODocument);

    res =
        db.query("select rel as el " + " from (select rel from Test)").stream()
            .map(OResult::toElement)
            .toList();

    Assert.assertEquals(res.size(), 1);
    ele = res.get(0);
    Assert.assertTrue(ele.getProperty("el") instanceof Set<?>);
  }
}
