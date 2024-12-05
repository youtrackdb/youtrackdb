package com.orientechnologies.lucene.test;

import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneNullTest extends BaseLuceneTest {

  @Test
  public void testNullChangeToNotNullWithLists() {

    db.command("create class Test extends V").close();

    db.command("create property Test.names EMBEDDEDLIST STRING").close();

    db.command("create index Test.names on Test (names) fulltext engine lucene").close();

    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    db.save(doc);
    db.commit();

    db.begin();
    OIndex index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    db.command("create class Test extends V").close();
    db.command("create property Test.names EMBEDDEDLIST STRING").close();
    db.command("create index Test.names on Test (names) fulltext engine lucene").close();

    YTEntityImpl doc = new YTEntityImpl("Test");

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
    Assert.assertEquals(index.getInternal().size(db), 0);
    db.commit();
  }
}
