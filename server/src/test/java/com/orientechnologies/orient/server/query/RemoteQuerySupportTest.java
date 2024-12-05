package com.orientechnologies.orient.server.query;

import static com.orientechnologies.core.config.YTGlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTSerializationException;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.server.BaseServerMemoryDatabase;
import com.orientechnologies.orient.server.OClientConnection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class RemoteQuerySupportTest extends BaseServerMemoryDatabase {

  private int oldPageSize;

  public void beforeTest() {
    super.beforeTest();
    db.createClass("Some");
    oldPageSize = QUERY_REMOTE_RESULTSET_PAGE_SIZE.getValueAsInteger();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(10);
  }

  @Test
  public void testQuery() {
    for (int i = 0; i < 150; i++) {
      db.begin();
      YTEntityImpl doc = new YTEntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    YTResultSet res = db.query("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      YTResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandSelect() {
    for (int i = 0; i < 150; i++) {
      db.begin();
      YTEntityImpl doc = new YTEntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    YTResultSet res = db.command("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      YTResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandInsertWithPageOverflow() {
    for (int i = 0; i < 150; i++) {
      db.begin();
      YTEntityImpl doc = new YTEntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    db.begin();
    YTResultSet res = db.command("insert into V from select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      YTResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
    db.commit();
  }

  @Test(expected = YTDatabaseException.class)
  public void testQueryKilledSession() {
    for (int i = 0; i < 150; i++) {
      YTEntityImpl doc = new YTEntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }
    YTResultSet res = db.query("select from Some");

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    db.activateOnCurrentThread();

    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      YTResult item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testQueryEmbedded() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Some");
    doc.setProperty("prop", "value");
    YTEntityImpl emb = new YTEntityImpl();
    emb.setProperty("one", "value");
    doc.setProperty("emb", emb, YTType.EMBEDDED);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select emb from Some");

    YTResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    assertEquals(((YTResult) item.getProperty("emb")).getProperty("one"), "value");
  }

  @Test
  public void testQueryDoubleEmbedded() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Some");
    doc.setProperty("prop", "value");
    YTEntityImpl emb1 = new YTEntityImpl();
    emb1.setProperty("two", "value");
    YTEntityImpl emb = new YTEntityImpl();
    emb.setProperty("one", "value");
    emb.setProperty("secEmb", emb1, YTType.EMBEDDED);

    doc.setProperty("emb", emb, YTType.EMBEDDED);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select emb from Some");

    YTResult item = res.next();
    assertNotNull(item.getProperty("emb"));
    YTResult resEmb = item.getProperty("emb");
    assertEquals(resEmb.getProperty("one"), "value");
    assertEquals(((YTResult) resEmb.getProperty("secEmb")).getProperty("two"), "value");
  }

  @Test
  public void testQueryEmbeddedList() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Some");
    doc.setProperty("prop", "value");
    YTEntityImpl emb = new YTEntityImpl();
    emb.setProperty("one", "value");
    List<Object> list = new ArrayList<>();
    list.add(emb);
    doc.setProperty("list", list, YTType.EMBEDDEDLIST);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select list from Some");

    YTResult item = res.next();
    assertNotNull(item.getProperty("list"));
    assertEquals(((List<YTResult>) item.getProperty("list")).size(), 1);
    assertEquals(((List<YTResult>) item.getProperty("list")).get(0).getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedSet() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Some");
    doc.setProperty("prop", "value");
    YTEntityImpl emb = new YTEntityImpl();
    emb.setProperty("one", "value");
    Set<YTEntityImpl> set = new HashSet<>();
    set.add(emb);
    doc.setProperty("set", set, YTType.EMBEDDEDSET);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select set from Some");

    YTResult item = res.next();
    assertNotNull(item.getProperty("set"));
    assertEquals(((Set<YTResult>) item.getProperty("set")).size(), 1);
    assertEquals(
        ((Set<YTResult>) item.getProperty("set")).iterator().next().getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedMap() {
    db.begin();
    YTEntityImpl doc = new YTEntityImpl("Some");
    doc.setProperty("prop", "value");
    YTEntityImpl emb = new YTEntityImpl();
    emb.setProperty("one", "value");
    Map<String, YTEntityImpl> map = new HashMap<>();
    map.put("key", emb);
    doc.setProperty("map", map, YTType.EMBEDDEDMAP);
    db.save(doc);
    db.commit();

    YTResultSet res = db.query("select map from Some");

    YTResult item = res.next();
    assertNotNull(item.getProperty("map"));
    assertEquals(((Map<String, YTResult>) item.getProperty("map")).size(), 1);
    assertEquals(
        ((Map<String, YTResult>) item.getProperty("map")).get("key").getProperty("one"), "value");
  }

  @Test
  public void testCommandWithTX() {

    db.begin();

    db.command("insert into Some set prop = 'value'");

    YTRecord record;

    try (YTResultSet resultSet = db.command("insert into Some set prop = 'value'")) {
      record = resultSet.next().getRecord().get();
    }

    db.commit();

    Assert.assertTrue(record.getIdentity().isPersistent());
  }

  @Test(expected = YTSerializationException.class)
  public void testBrokenParameter() {
    try {
      db.query("select from Some where prop= ?", new Object()).close();
    } catch (RuntimeException e) {
      // should be possible to run a query after without getting the server stuck
      db.query("select from Some where prop= ?", new YTRecordId(10, 10)).close();
      throw e;
    }
  }

  @Test
  public void testScriptWithRidbags() {
    db.command("create class testScriptWithRidbagsV extends V");
    db.command("create class testScriptWithRidbagsE extends E");

    db.begin();
    db.command("create vertex testScriptWithRidbagsV set name = 'a'");
    db.command("create vertex testScriptWithRidbagsV set name = 'b'");

    db.command(
        "create edge testScriptWithRidbagsE from (select from testScriptWithRidbagsV where name ="
            + " 'a') TO (select from testScriptWithRidbagsV where name = 'b');");
    db.commit();

    String script = "";
    script += "BEGIN;";
    script += "LET q1 = SELECT * FROM testScriptWithRidbagsV WHERE name = 'a';";
    script += "LET q2 = SELECT * FROM testScriptWithRidbagsV WHERE name = 'b';";
    script += "COMMIT ;";
    script += "RETURN [$q1,$q2]";

    YTResultSet rs = db.execute("sql", script);

    rs.forEachRemaining(System.out::println);
    rs.close();
  }

  @Test
  public void testLetOut() {
    db.command("create class letVertex extends V");
    db.command("create class letEdge extends E");

    db.begin();
    db.command("create vertex letVertex set name = 'a'");
    db.command("create vertex letVertex set name = 'b'");
    db.command(
        "create edge letEdge from (select from letVertex where name = 'a') TO (select from"
            + " letVertex where name = 'b');");
    db.commit();

    YTResultSet rs =
        db.query("select $someNode.in('letEdge') from letVertex LET $someNode =out('letEdge');");
    assertEquals(rs.stream().count(), 2);
  }

  public void afterTest() {
    super.afterTest();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(oldPageSize);
  }
}
