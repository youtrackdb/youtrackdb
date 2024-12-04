package com.orientechnologies.lucene.tests;

import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class OLuceneNullTest extends OLuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    db.command("create class Test extends V");

    db.command("create property Test.names EMBEDDEDLIST STRING");

    db.command("create index Test.names on Test(names) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testNullChangeToNotNullWithLists() {

    db.begin();
    YTDocument doc = new YTDocument("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    db.save(doc);
    db.commit();

    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");

    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    YTDocument doc = new YTDocument("Test");

    db.begin();
    doc.field("names", new String[]{"foo"});
    db.save(doc);
    db.commit();

    db.begin();

    doc = db.bindToSession(doc);
    doc.removeField("names");

    db.save(doc);
    db.commit();

    db.begin();
    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");
    Assert.assertEquals(0, index.getInternal().size(db));
    db.commit();
  }
}
