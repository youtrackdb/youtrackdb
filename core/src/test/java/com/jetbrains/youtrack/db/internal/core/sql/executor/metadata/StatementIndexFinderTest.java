package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.jetbrains.youtrack.db.api.YouTrackDB;
import com.jetbrains.youtrack.db.api.config.YouTrackDBConfig;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaClass.INDEX_TYPE;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.metadata.IndexFinder.Operation;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ParseException;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SQLSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SimpleNode;
import com.jetbrains.youtrack.db.internal.core.sql.parser.YouTrackDBSql;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class StatementIndexFinderTest {

  private DatabaseSessionInternal session;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    this.youTrackDb = new YouTrackDBImpl(DbTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    this.youTrackDb.execute(
        "create database "
            + StatementIndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        (DatabaseSessionInternal)
            this.youTrackDb.open(
                StatementIndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void simpleMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    SQLSelectStatement stat = parseQuery("select from cl where name='a'");
    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleRangeTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    SQLSelectStatement stat = parseQuery("select from cl where name > 'a'");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Gt, result.get().getOperation());

    SQLSelectStatement stat1 = parseQuery("select from cl where name < 'a'");
    Optional<IndexCandidate> result1 = stat1.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result1.get().getName());
    assertEquals(Operation.Lt, result1.get().getOperation());
  }

  @Test
  public void multipleSimpleAndMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    SQLSelectStatement stat = parseQuery("select from cl where name='a' and name='b'");
    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof MultipleIndexCanditate));
    MultipleIndexCanditate multiple = (MultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
  }

  @Test
  public void requiredRangeOrMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    SQLSelectStatement stat = parseQuery("select from cl where name='a' or name='b'");
    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof RequiredIndexCanditate));
    RequiredIndexCanditate required = (RequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void multipleRangeAndTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof MultipleIndexCanditate));
    MultipleIndexCanditate multiple = (MultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, multiple.getCanditates().get(1).getOperation());
  }

  @Test
  public void requiredRangeOrTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat = parseQuery("select from cl where name < 'a' or name > 'b'");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof RequiredIndexCanditate));
    RequiredIndexCanditate required = (RequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleRangeNotTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat = parseQuery("select from cl where not name < 'a' ");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Ge, result.get().getOperation());
  }

  @Test
  public void simpleChainTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    SchemaProperty prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat = parseQuery("select from cl where friend.friend.name = 'a' ");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleNestedAndOrMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    SchemaProperty prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof RequiredIndexCanditate));
    RequiredIndexCanditate required = (RequiredIndexCanditate) result.get();
    assertTrue((required.getCanditates().get(0) instanceof MultipleIndexCanditate));
    MultipleIndexCanditate first = (MultipleIndexCanditate) required.getCanditates().get(0);
    assertEquals("cl.friend->cl.name->", first.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(0).getOperation());
    assertEquals("cl.name", first.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(1).getOperation());

    MultipleIndexCanditate second = (MultipleIndexCanditate) required.getCanditates().get(1);
    assertEquals("cl.friend->cl.name->", second.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(0).getOperation());
    assertEquals("cl.name", second.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleNestedAndOrPartialMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof RequiredIndexCanditate));
    RequiredIndexCanditate required = (RequiredIndexCanditate) result.get();
    IndexCandidate first = required.getCanditates().get(0);
    assertEquals("cl.name", first.getName());
    assertEquals(Operation.Eq, first.getOperation());

    IndexCandidate second = required.getCanditates().get(1);
    assertEquals("cl.name", second.getName());
    assertEquals(Operation.Eq, second.getOperation());
  }

  @Test
  public void simpleNestedOrNotMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    SchemaProperty prop = cl.createProperty(session, "name", PropertyType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    SchemaProperty prop1 = cl.createProperty(session, "friend", PropertyType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    SQLSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.other='b' and"
                + " other='b') ");
    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    SQLSelectStatement stat = parseQuery("select from cl where name = 'a' and surname = 'b'");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueMatchOneTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    SQLSelectStatement stat = parseQuery("select from cl where name = 'a' and other = 'b'");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueNotMatchSecondPropertyTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createProperty(session, "other", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname_other", INDEX_TYPE.NOTUNIQUE, "name", "surname",
        "other");

    SQLSelectStatement stat = parseQuery("select from cl where surname = 'a' and other = 'b'");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueNotMatchSecondPropertySingleConditionTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    SQLSelectStatement stat = parseQuery("select from cl where surname = 'a'");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchPropertyORTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    SQLSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (name='d' and surname='e')");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue(result.isPresent());
    assertTrue((result.get() instanceof RequiredIndexCanditate));
    RequiredIndexCanditate required = (RequiredIndexCanditate) result.get();
    assertEquals("cl.name_surname", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name_surname", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
    assertEquals(required.getCanditates().size(), 2);
  }

  @Test
  public void multivalueNotMatchPropertyORTest() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createProperty(session, "surname", PropertyType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    SQLSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (other='d' and surname='e')");

    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMutipleConditionBetween() {
    SchemaClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", PropertyType.STRING);
    cl.createIndex(session, "cl.name", INDEX_TYPE.NOTUNIQUE, "name");

    SQLSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    IndexFinder finder = new ClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<IndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue((result.get() instanceof RangeIndexCanditate));
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Range, result.get().getOperation());
  }

  private SQLSelectStatement parseQuery(String query) {
    InputStream is = new ByteArrayInputStream(query.getBytes());
    YouTrackDBSql osql = new YouTrackDBSql(is);
    try {
      SimpleNode n = osql.parse();
      return (SQLSelectStatement) n;
    } catch (ParseException e) {
      throw new RuntimeException(e);
    }
  }

  @After
  public void after() {
    this.session.close();
    this.youTrackDb.close();
  }
}
