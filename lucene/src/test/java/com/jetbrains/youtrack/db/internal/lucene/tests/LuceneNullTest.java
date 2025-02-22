package com.jetbrains.youtrack.db.internal.lucene.tests;

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
    session.command("create class Test extends V");

    session.command("create property Test.names EMBEDDEDLIST STRING");

    session.command("create index Test.names on Test(names) FULLTEXT ENGINE LUCENE");
  }

  @Test
  public void testNullChangeToNotNullWithLists() {

    session.begin();
    var doc = ((EntityImpl) session.newEntity("Test"));
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    session.commit();

    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "Test.names");

    session.begin();
    Assert.assertEquals(1, index.getInternal().size(session));
    session.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    var doc = ((EntityImpl) session.newEntity("Test"));

    session.begin();
    doc.field("names", new String[]{"foo"});
    session.commit();

    session.begin();

    doc = session.bindToSession(doc);
    doc.removeField("names");

    session.commit();

    session.begin();
    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "Test.names");
    Assert.assertEquals(0, index.getInternal().size(session));
    session.commit();
  }
}
