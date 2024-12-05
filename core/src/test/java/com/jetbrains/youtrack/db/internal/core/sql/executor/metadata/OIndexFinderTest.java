package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDB;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTProperty;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OIndexFinderTest {

  private YTDatabaseSessionInternal session;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    this.youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    this.youTrackDb.execute(
        "create database "
            + OIndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        (YTDatabaseSessionInternal)
            this.youTrackDb.open(OIndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void testFindSimpleMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "surname", YTType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindSimpleMatchHashIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    YTProperty prop1 = cl.createProperty(session, "surname", YTType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "surname", YTType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result =
        finder.findAllowRangeIndex(new OPath("name"), Operation.Ge, null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 =
        finder.findAllowRangeIndex(new OPath("surname"), Operation.Ge, null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeNotMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    YTProperty prop1 = cl.createProperty(session, "surname", YTType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);
    YTProperty prop2 = cl.createProperty(session, "third", YTType.STRING);
    prop2.createIndex(session, INDEX_TYPE.FULLTEXT);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result =
        finder.findAllowRangeIndex(new OPath("name"), Operation.Ge, null, ctx);

    assertFalse(result.isPresent());

    Optional<OIndexCandidate> result1 =
        finder.findAllowRangeIndex(new OPath("surname"), Operation.Ge, null, ctx);

    assertFalse(result1.isPresent());

    Optional<OIndexCandidate> result2 =
        finder.findAllowRangeIndex(new OPath("third"), Operation.Ge, null, ctx);

    assertFalse(result2.isPresent());
  }

  @Test
  public void testFindByKey() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", YTType.EMBEDDEDMAP);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findByKeyIndex(new OPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindByValue() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", YTType.EMBEDDEDMAP, YTType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findByValueIndex(new OPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindFullTextMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findFullTextIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());
  }

  @Test
  public void testFindChainMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findExactIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainRangeIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findAllowRangeIndex(path, Operation.Ge, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainByKeyIndex() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", YTType.EMBEDDEDMAP, YTType.STRING);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    OPath path = new OPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findByKeyIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainByValueIndex() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", YTType.EMBEDDEDMAP, YTType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    OPath path = new OPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findByValueIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainFullTextMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");

    Optional<OIndexCandidate> result = finder.findFullTextIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindMultivalueMatchIndex() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name_surname", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.name_surname", result1.get().getName());
  }

  @After
  public void after() {
    this.session.close();
    this.youTrackDb.close();
  }
}
