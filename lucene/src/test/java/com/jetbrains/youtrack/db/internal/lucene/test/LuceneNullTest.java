package com.jetbrains.youtrack.db.internal.lucene.test;

import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class LuceneNullTest extends BaseLuceneTest {

  @Test
  public void testNullChangeToNotNullWithLists() {

    session.command("create class Test extends V").close();

    session.command("create property Test.names EMBEDDEDLIST STRING").close();

    session.command("create index Test.names on Test (names) fulltext engine lucene").close();

    session.begin();
    var doc = ((EntityImpl) session.newEntity("Test"));
    session.commit();

    session.begin();
    doc = session.bindToSession(doc);
    doc.field("names", new String[]{"foo"});
    session.commit();

    session.begin();
    var index = session.getMetadata().getIndexManagerInternal().getIndex(session, "Test.names");
    Assert.assertEquals(1, index.getInternal().size(session));
    session.commit();
  }

  @Test
  public void testNotNullChangeToNullWithLists() {

    session.command("create class Test extends V").close();
    session.command("create property Test.names EMBEDDEDLIST STRING").close();
    session.command("create index Test.names on Test (names) fulltext engine lucene").close();

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
    Assert.assertEquals(index.getInternal().size(session), 0);
    session.commit();
  }
}
