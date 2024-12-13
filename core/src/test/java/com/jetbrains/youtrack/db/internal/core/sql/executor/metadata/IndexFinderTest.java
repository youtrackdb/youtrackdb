package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.schema.Property;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class IndexFinderTest {

  private DatabaseSessionInternal session;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    this.youTrackDb = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    this.youTrackDb.execute(
        "create database "
            + IndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        (DatabaseSessionInternal)
            this.youTrackDb.open(IndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void testFindSimpleMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    Property prop1 = cl.createProperty(session, "surname", PropertyType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findExactIndex(new MetadataPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<IndexCandidate> result1 = finder.findExactIndex(new MetadataPath("surname"), null,
        ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindSimpleMatchHashIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    Property prop1 = cl.createProperty(session, "surname", PropertyType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findExactIndex(new MetadataPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<IndexCandidate> result1 = finder.findExactIndex(new MetadataPath("surname"), null,
        ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    Property prop1 = cl.createProperty(session, "surname", PropertyType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result =
        finder.findAllowRangeIndex(new MetadataPath("name"), Operation.Ge, null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<IndexCandidate> result1 =
        finder.findAllowRangeIndex(new MetadataPath("surname"), Operation.Ge, null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeNotMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    Property prop1 = cl.createProperty(session, "surname", PropertyType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);
    Property prop2 = cl.createProperty(session, "third", PropertyType.STRING);
    prop2.createIndex(session, INDEX_TYPE.FULLTEXT);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result =
        finder.findAllowRangeIndex(new MetadataPath("name"), Operation.Ge, null, ctx);

    assertFalse(result.isPresent());

    Optional<IndexCandidate> result1 =
        finder.findAllowRangeIndex(new MetadataPath("surname"), Operation.Ge, null, ctx);

    assertFalse(result1.isPresent());

    Optional<IndexCandidate> result2 =
        finder.findAllowRangeIndex(new MetadataPath("third"), Operation.Ge, null, ctx);

    assertFalse(result2.isPresent());
  }

  @Test
  public void testFindByKey() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", PropertyType.EMBEDDEDMAP);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findByKeyIndex(new MetadataPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindByValue() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", PropertyType.EMBEDDEDMAP, PropertyType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findByValueIndex(new MetadataPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindFullTextMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findFullTextIndex(new MetadataPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());
  }

  @Test
  public void testFindChainMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    Property prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    MetadataPath path = new MetadataPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<IndexCandidate> result = finder.findExactIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainRangeIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    Property prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    MetadataPath path = new MetadataPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<IndexCandidate> result = finder.findAllowRangeIndex(path, Operation.Ge, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainByKeyIndex() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", PropertyType.EMBEDDEDMAP, PropertyType.STRING);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();
    Property prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    MetadataPath path = new MetadataPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<IndexCandidate> result = finder.findByKeyIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainByValueIndex() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", PropertyType.EMBEDDEDMAP, PropertyType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();
    Property prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    MetadataPath path = new MetadataPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<IndexCandidate> result = finder.findByValueIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainFullTextMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    Property prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);
    Property prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    MetadataPath path = new MetadataPath("name");
    path.addPre("friend");
    path.addPre("friend");

    Optional<IndexCandidate> result = finder.findFullTextIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindMultivalueMatchIndex() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = finder.findExactIndex(new MetadataPath("name"), null, ctx);

    assertEquals("cl.name_surname", result.get().getName());

    Optional<IndexCandidate> result1 = finder.findExactIndex(new MetadataPath("surname"), null,
        ctx);

    assertEquals("cl.name_surname", result1.get().getName());
  }

  @After
  public void after() {
    this.session.close();
    this.youTrackDb.close();
  }
}
