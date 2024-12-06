package com.orientechnologies.orient.server.query;

import static com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration.QUERY_REMOTE_RESULTSET_PAGE_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.internal.core.exception.SerializationException;
import com.jetbrains.youtrack.db.internal.core.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
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
      EntityImpl doc = new EntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    ResultSet res = db.query("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      Result item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandSelect() {
    for (int i = 0; i < 150; i++) {
      db.begin();
      EntityImpl doc = new EntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    ResultSet res = db.command("select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      Result item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testCommandInsertWithPageOverflow() {
    for (int i = 0; i < 150; i++) {
      db.begin();
      EntityImpl doc = new EntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
      db.commit();
    }

    db.begin();
    ResultSet res = db.command("insert into V from select from Some");
    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      Result item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
    db.commit();
  }

  @Test(expected = DatabaseException.class)
  public void testQueryKilledSession() {
    for (int i = 0; i < 150; i++) {
      EntityImpl doc = new EntityImpl("Some");
      doc.setProperty("prop", "value");
      db.save(doc);
    }
    ResultSet res = db.query("select from Some");

    for (OClientConnection conn : server.getClientConnectionManager().getConnections()) {
      conn.close();
    }
    db.activateOnCurrentThread();

    for (int i = 0; i < 150; i++) {
      assertTrue(res.hasNext());
      Result item = res.next();
      assertEquals(item.getProperty("prop"), "value");
    }
  }

  @Test
  public void testQueryEmbedded() {
    db.begin();
    EntityImpl doc = new EntityImpl("Some");
    doc.setProperty("prop", "value");
    EntityImpl emb = new EntityImpl();
    emb.setProperty("one", "value");
    doc.setProperty("emb", emb, PropertyType.EMBEDDED);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select emb from Some");

    Result item = res.next();
    assertNotNull(item.getProperty("emb"));
    assertEquals(((Result) item.getProperty("emb")).getProperty("one"), "value");
  }

  @Test
  public void testQueryDoubleEmbedded() {
    db.begin();
    EntityImpl doc = new EntityImpl("Some");
    doc.setProperty("prop", "value");
    EntityImpl emb1 = new EntityImpl();
    emb1.setProperty("two", "value");
    EntityImpl emb = new EntityImpl();
    emb.setProperty("one", "value");
    emb.setProperty("secEmb", emb1, PropertyType.EMBEDDED);

    doc.setProperty("emb", emb, PropertyType.EMBEDDED);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select emb from Some");

    Result item = res.next();
    assertNotNull(item.getProperty("emb"));
    Result resEmb = item.getProperty("emb");
    assertEquals(resEmb.getProperty("one"), "value");
    assertEquals(((Result) resEmb.getProperty("secEmb")).getProperty("two"), "value");
  }

  @Test
  public void testQueryEmbeddedList() {
    db.begin();
    EntityImpl doc = new EntityImpl("Some");
    doc.setProperty("prop", "value");
    EntityImpl emb = new EntityImpl();
    emb.setProperty("one", "value");
    List<Object> list = new ArrayList<>();
    list.add(emb);
    doc.setProperty("list", list, PropertyType.EMBEDDEDLIST);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select list from Some");

    Result item = res.next();
    assertNotNull(item.getProperty("list"));
    assertEquals(((List<Result>) item.getProperty("list")).size(), 1);
    assertEquals(((List<Result>) item.getProperty("list")).get(0).getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedSet() {
    db.begin();
    EntityImpl doc = new EntityImpl("Some");
    doc.setProperty("prop", "value");
    EntityImpl emb = new EntityImpl();
    emb.setProperty("one", "value");
    Set<EntityImpl> set = new HashSet<>();
    set.add(emb);
    doc.setProperty("set", set, PropertyType.EMBEDDEDSET);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select set from Some");

    Result item = res.next();
    assertNotNull(item.getProperty("set"));
    assertEquals(((Set<Result>) item.getProperty("set")).size(), 1);
    assertEquals(
        ((Set<Result>) item.getProperty("set")).iterator().next().getProperty("one"), "value");
  }

  @Test
  public void testQueryEmbeddedMap() {
    db.begin();
    EntityImpl doc = new EntityImpl("Some");
    doc.setProperty("prop", "value");
    EntityImpl emb = new EntityImpl();
    emb.setProperty("one", "value");
    Map<String, EntityImpl> map = new HashMap<>();
    map.put("key", emb);
    doc.setProperty("map", map, PropertyType.EMBEDDEDMAP);
    db.save(doc);
    db.commit();

    ResultSet res = db.query("select map from Some");

    Result item = res.next();
    assertNotNull(item.getProperty("map"));
    assertEquals(((Map<String, Result>) item.getProperty("map")).size(), 1);
    assertEquals(
        ((Map<String, Result>) item.getProperty("map")).get("key").getProperty("one"), "value");
  }

  @Test
  public void testCommandWithTX() {

    db.begin();

    db.command("insert into Some set prop = 'value'");

    Record record;

    try (ResultSet resultSet = db.command("insert into Some set prop = 'value'")) {
      record = resultSet.next().getRecord().get();
    }

    db.commit();

    Assert.assertTrue(record.getIdentity().isPersistent());
  }

  @Test(expected = SerializationException.class)
  public void testBrokenParameter() {
    try {
      db.query("select from Some where prop= ?", new Object()).close();
    } catch (RuntimeException e) {
      // should be possible to run a query after without getting the server stuck
      db.query("select from Some where prop= ?", new RecordId(10, 10)).close();
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

    ResultSet rs = db.execute("sql", script);

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

    ResultSet rs =
        db.query("select $someNode.in('letEdge') from letVertex LET $someNode =out('letEdge');");
    assertEquals(rs.stream().count(), 2);
  }

  public void afterTest() {
    super.afterTest();
    QUERY_REMOTE_RESULTSET_PAGE_SIZE.setValue(oldPageSize);
  }
}
