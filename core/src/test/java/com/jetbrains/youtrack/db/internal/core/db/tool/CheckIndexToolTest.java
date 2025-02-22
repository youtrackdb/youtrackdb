package com.jetbrains.youtrack.db.internal.core.db.tool;

import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.BaseMemoryInternalDatabase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class CheckIndexToolTest extends BaseMemoryInternalDatabase {

  @Test
  public void test() {
    session.command("create class Foo").close();
    session.command("create property Foo.name STRING").close();
    session.command("create index Foo.name on Foo (name) NOTUNIQUE").close();

    session.begin();
    EntityImpl doc = session.newInstance("Foo");
    doc.field("name", "a");

    session.commit();

    RID rid = doc.getIdentity();

    var N_RECORDS = 100000;
    for (var i = 0; i < N_RECORDS; i++) {
      session.begin();
      doc = session.newInstance("Foo");
      doc.field("name", "x" + i);

      session.commit();
    }

    session.begin();
    var idx = session.getMetadata().getIndexManagerInternal().getIndex(session, "Foo.name");
    var key = idx.getDefinition().createValue(session, "a");
    idx.remove(session, key, rid);
    session.commit();

    session.begin();
    var result = session.query("SELECT FROM Foo");
    Assert.assertEquals(N_RECORDS + 1, result.stream().count());

    var tool = new CheckIndexTool();
    tool.setDatabaseSession(session);
    tool.setVerbose(true);
    tool.setOutputListener(System.out::println);

    tool.run();
    session.commit();

    Assert.assertEquals(1, tool.getTotalErrors());
  }

  @Test
  public void testBugOnCollectionIndex() {
    session.command("create class testclass");
    session.command("create property testclass.name string");
    session.command("create property testclass.tags linklist");
    session.command("alter property testclass.tags default '[]'");
    session.command("create index testclass_tags_idx on testclass (tags) NOTUNIQUE");

    session.begin();
    session.command("insert into testclass set name = 'a',tags = [#5:0] ");
    session.command("insert into testclass set name = 'b'");
    session.command("insert into testclass set name = 'c' ");
    session.commit();

    final var tool = new CheckIndexTool();

    tool.setDatabaseSession(session);
    tool.setVerbose(true);
    tool.setOutputListener(System.out::println);
    tool.run();
    Assert.assertEquals(0, tool.getTotalErrors());
  }
}
