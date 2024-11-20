package com.orientechnologies.orient.core.db.tool;

import com.orientechnologies.BaseMemoryInternalDatabase;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.Assert;
import org.junit.Test;

/**
 * Created by luigidellaquila on 14/09/17.
 */
public class OCheckIndexToolTest extends BaseMemoryInternalDatabase {

  @Test
  public void test() {
    db.command("create class Foo").close();
    db.command("create property Foo.name STRING").close();
    db.command("create index Foo.name on Foo (name) NOTUNIQUE").close();

    db.begin();
    ODocument doc = db.newInstance("Foo");
    doc.field("name", "a");
    doc.save();
    db.commit();

    ORID rid = doc.getIdentity();

    int N_RECORDS = 100000;
    for (int i = 0; i < N_RECORDS; i++) {
      db.begin();
      doc = db.newInstance("Foo");
      doc.field("name", "x" + i);
      doc.save();
      db.commit();
    }

    OIndex idx = db.getMetadata().getIndexManagerInternal().getIndex(db, "Foo.name");
    Object key = idx.getDefinition().createValue(db, "a");
    idx.remove(key, rid);

    OResultSet result = db.query("SELECT FROM Foo");
    Assert.assertEquals(N_RECORDS + 1, result.stream().count());

    OCheckIndexTool tool = new OCheckIndexTool();
    tool.setDatabase(db);
    tool.setVerbose(true);
    tool.setOutputListener(System.out::println);

    tool.run();
    Assert.assertEquals(1, tool.getTotalErrors());
  }

  @Test
  public void testBugOnCollectionIndex() {
    db.command("create class testclass");
    db.command("create property testclass.name string");
    db.command("create property testclass.tags linklist");
    db.command("alter property testclass.tags default '[]'");
    db.command("create index testclass_tags_idx on testclass (tags) NOTUNIQUE_HASH_INDEX");

    db.begin();
    db.command("insert into testclass set name = 'a',tags = [#5:0] ");
    db.command("insert into testclass set name = 'b'");
    db.command("insert into testclass set name = 'c' ");
    db.commit();

    final OCheckIndexTool tool = new OCheckIndexTool();

    tool.setDatabase(db);
    tool.setVerbose(true);
    tool.setOutputListener(System.out::println);
    tool.run();
    Assert.assertEquals(0, tool.getTotalErrors());
  }
}
