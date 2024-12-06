package com.orientechnologies.lucene.test;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
    EntityImpl doc = new EntityImpl("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    db.save(doc);
    db.commit();

    db.begin();
    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    db.command("create class Test extends V").close();
    db.command("create property Test.names EMBEDDEDLIST STRING").close();
    db.command("create index Test.names on Test (names) fulltext engine lucene").close();

    EntityImpl doc = new EntityImpl("Test");

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
    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");
    Assert.assertEquals(index.getInternal().size(db), 0);
    db.commit();
  }
}
