package com.orientechnologies.orient.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.orient.core.command.OBasicCommandContext;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.OxygenDB;
import com.orientechnologies.orient.core.db.OxygenDBConfig;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OClass.INDEX_TYPE;
import com.orientechnologies.orient.core.metadata.schema.OProperty;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.sql.executor.metadata.OIndexFinder.Operation;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OIndexFinderTest {

  private ODatabaseSessionInternal session;
  private OxygenDB oxygenDb;

  @Before
  public void before() {
    this.oxygenDb = new OxygenDB("embedded:", OxygenDBConfig.defaultConfig());
    this.oxygenDb.execute(
        "create database "
            + OIndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        (ODatabaseSessionInternal)
            this.oxygenDb.open(OIndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void testFindSimpleMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty(session, "surname", OType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindSimpleMatchHashIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    OProperty prop1 = cl.createProperty(session, "surname", OType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty(session, "surname", OType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result =
        finder.findAllowRangeIndex(new OPath("name"), Operation.Ge, null, ctx);

    assertEquals("cl.name", result.get().getName());

    Optional<OIndexCandidate> result1 =
        finder.findAllowRangeIndex(new OPath("surname"), Operation.Ge, null, ctx);

    assertEquals("cl.surname", result1.get().getName());
  }

  @Test
  public void testFindRangeNotMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE_HASH_INDEX);
    OProperty prop1 = cl.createProperty(session, "surname", OType.STRING);
    prop1.createIndex(session, INDEX_TYPE.UNIQUE_HASH_INDEX);
    OProperty prop2 = cl.createProperty(session, "third", OType.STRING);
    prop2.createIndex(session, INDEX_TYPE.FULLTEXT);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
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
    OClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", OType.EMBEDDEDMAP);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findByKeyIndex(new OPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindByValue() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", OType.EMBEDDEDMAP, OType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findByValueIndex(new OPath("map"), null, ctx);

    assertEquals("cl.map", result.get().getName());
  }

  @Test
  public void testFindFullTextMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findFullTextIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name", result.get().getName());
  }

  @Test
  public void testFindChainMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty(session, "friend", OType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findExactIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainRangeIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    OProperty prop1 = cl.createProperty(session, "friend", OType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findAllowRangeIndex(path, Operation.Ge, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindChainByKeyIndex() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", OType.EMBEDDEDMAP, OType.STRING);
    this.session.command("create index cl.map on cl(map by key) NOTUNIQUE").close();
    OProperty prop1 = cl.createProperty(session, "friend", OType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    OPath path = new OPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findByKeyIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainByValueIndex() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty(session, "map", OType.EMBEDDEDMAP, OType.STRING);
    this.session.command("create index cl.map on cl(map by value) NOTUNIQUE").close();
    OProperty prop1 = cl.createProperty(session, "friend", OType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    OPath path = new OPath("map");
    path.addPre("friend");
    path.addPre("friend");
    Optional<OIndexCandidate> result = finder.findByValueIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.map->", result.get().getName());
  }

  @Test
  public void testFindChainFullTextMatchIndex() {
    OClass cl = this.session.createClass("cl");
    OProperty prop = cl.createProperty(session, "name", OType.STRING);
    prop.createIndex(session, INDEX_TYPE.FULLTEXT);
    OProperty prop1 = cl.createProperty(session, "friend", OType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    OPath path = new OPath("name");
    path.addPre("friend");
    path.addPre("friend");

    Optional<OIndexCandidate> result = finder.findFullTextIndex(path, null, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
  }

  @Test
  public void testFindMultivalueMatchIndex() {
    OClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", OType.STRING);
    cl.createProperty(session, "surname", OType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OIndexFinder finder = new OClassIndexFinder("cl");
    OBasicCommandContext ctx = new OBasicCommandContext(session);
    Optional<OIndexCandidate> result = finder.findExactIndex(new OPath("name"), null, ctx);

    assertEquals("cl.name_surname", result.get().getName());

    Optional<OIndexCandidate> result1 = finder.findExactIndex(new OPath("surname"), null, ctx);

    assertEquals("cl.name_surname", result1.get().getName());
  }

  @After
  public void after() {
    this.session.close();
    this.oxygenDb.close();
  }
}
