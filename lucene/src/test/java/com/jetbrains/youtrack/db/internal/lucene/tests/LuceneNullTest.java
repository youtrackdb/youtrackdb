package com.jetbrains.youtrack.db.internal.lucene.tests;

import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class LuceneNullTest extends LuceneBaseTest {

  @Before
  public void setUp() throws Exception {
    db.command("create class Test extends V");

    db.command("create property Test.names EMBEDDEDLIST STRING");

    db.command("create index Test.names on Test(names) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testNullChangeToNotNullWithLists() {

    db.begin();
    EntityImpl doc = new EntityImpl("Test");
    db.save(doc);
    db.commit();

    db.begin();
    doc = db.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    db.save(doc);
    db.commit();

    Index index = db.getMetadata().getIndexManagerInternal().getIndex(db, "Test.names");

    db.begin();
    Assert.assertEquals(1, index.getInternal().size(db));
    db.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

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
    Assert.assertEquals(0, index.getInternal().size(db));
    db.commit();
  }
}
