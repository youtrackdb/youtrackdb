package com.jetbrains.youtrack.db.internal.core.sql.executor.metadata;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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
import com.jetbrains.youtrack.db.internal.core.sql.parser.OSelectStatement;
import com.jetbrains.youtrack.db.internal.core.sql.parser.OrientSql;
import com.jetbrains.youtrack.db.internal.core.sql.parser.ParseException;
import com.jetbrains.youtrack.db.internal.core.sql.parser.SimpleNode;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class OStatementIndexFinderTest {

  private YTDatabaseSessionInternal session;
  private YouTrackDB youTrackDb;

  @Before
  public void before() {
    this.youTrackDb = new YouTrackDB(DBTestBase.embeddedDBUrl(getClass()),
        YouTrackDBConfig.defaultConfig());
    this.youTrackDb.execute(
        "create database "
            + OStatementIndexFinderTest.class.getSimpleName()
            + " memory users (admin identified by 'adminpwd' role admin)");
    this.session =
        (YTDatabaseSessionInternal)
            this.youTrackDb.open(
                OStatementIndexFinderTest.class.getSimpleName(), "admin", "adminpwd");
  }

  @Test
  public void simpleMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleRangeTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name > 'a'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Gt, result.get().getOperation());

    OSelectStatement stat1 = parseQuery("select from cl where name < 'a'");
    Optional<OIndexCandidate> result1 = stat1.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result1.get().getName());
    assertEquals(Operation.Lt, result1.get().getOperation());
  }

  @Test
  public void multipleSimpleAndMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a' and name='b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate multiple = (OMultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, multiple.getCanditates().get(0).getOperation());
  }

  @Test
  public void requiredRangeOrMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OSelectStatement stat = parseQuery("select from cl where name='a' or name='b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void multipleRangeAndTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate multiple = (OMultipleIndexCanditate) result.get();
    assertEquals("cl.name", multiple.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, multiple.getCanditates().get(0).getOperation());
    assertEquals("cl.name", multiple.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, multiple.getCanditates().get(1).getOperation());
  }

  @Test
  public void requiredRangeOrTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where name < 'a' or name > 'b'");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name", required.getCanditates().get(0).getName());
    assertEquals(Operation.Lt, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name", required.getCanditates().get(1).getName());
    assertEquals(Operation.Gt, required.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleRangeNotTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where not name < 'a' ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Ge, result.get().getOperation());
  }

  @Test
  public void simpleChainTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat = parseQuery("select from cl where friend.friend.name = 'a' ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    assertEquals("cl.friend->cl.friend->cl.name->", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void simpleNestedAndOrMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertTrue((required.getCanditates().get(0) instanceof OMultipleIndexCanditate));
    OMultipleIndexCanditate first = (OMultipleIndexCanditate) required.getCanditates().get(0);
    assertEquals("cl.friend->cl.name->", first.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(0).getOperation());
    assertEquals("cl.name", first.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, first.getCanditates().get(1).getOperation());

    OMultipleIndexCanditate second = (OMultipleIndexCanditate) required.getCanditates().get(1);
    assertEquals("cl.friend->cl.name->", second.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(0).getOperation());
    assertEquals("cl.name", second.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, second.getCanditates().get(1).getOperation());
  }

  @Test
  public void simpleNestedAndOrPartialMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.name='b' and"
                + " name='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    OIndexCandidate first = required.getCanditates().get(0);
    assertEquals("cl.name", first.getName());
    assertEquals(Operation.Eq, first.getOperation());

    OIndexCandidate second = required.getCanditates().get(1);
    assertEquals("cl.name", second.getName());
    assertEquals(Operation.Eq, second.getOperation());
  }

  @Test
  public void simpleNestedOrNotMatchTest() {
    YTClass cl = this.session.createClass("cl");
    YTProperty prop = cl.createProperty(session, "name", YTType.STRING);
    prop.createIndex(session, INDEX_TYPE.NOTUNIQUE);
    YTProperty prop1 = cl.createProperty(session, "friend", YTType.LINK, cl);
    prop1.createIndex(session, INDEX_TYPE.NOTUNIQUE);

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    OSelectStatement stat =
        parseQuery(
            "select from cl where (friend.name = 'a' and name='a') or (friend.other='b' and"
                + " other='b') ");
    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);

    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where name = 'a' and surname = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueMatchOneTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where name = 'a' and other = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertEquals("cl.name_surname", result.get().getName());
    assertEquals(Operation.Eq, result.get().getOperation());
  }

  @Test
  public void multivalueNotMatchSecondPropertyTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createProperty(session, "other", YTType.STRING);
    cl.createIndex(session, "cl.name_surname_other", INDEX_TYPE.NOTUNIQUE, "name", "surname",
        "other");

    OSelectStatement stat = parseQuery("select from cl where surname = 'a' and other = 'b'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueNotMatchSecondPropertySingleConditionTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat = parseQuery("select from cl where surname = 'a'");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void multivalueMatchPropertyORTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (name='d' and surname='e')");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue(result.isPresent());
    assertTrue((result.get() instanceof ORequiredIndexCanditate));
    ORequiredIndexCanditate required = (ORequiredIndexCanditate) result.get();
    assertEquals("cl.name_surname", required.getCanditates().get(0).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(0).getOperation());
    assertEquals("cl.name_surname", required.getCanditates().get(1).getName());
    assertEquals(Operation.Eq, required.getCanditates().get(1).getOperation());
    assertEquals(required.getCanditates().size(), 2);
  }

  @Test
  public void multivalueNotMatchPropertyORTest() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createProperty(session, "surname", YTType.STRING);
    cl.createIndex(session, "cl.name_surname", INDEX_TYPE.NOTUNIQUE, "name", "surname");

    OSelectStatement stat =
        parseQuery(
            "select from cl where (name = 'a' and surname = 'b') or (other='d' and surname='e')");

    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertFalse(result.isPresent());
  }

  @Test
  public void testMutipleConditionBetween() {
    YTClass cl = this.session.createClass("cl");
    cl.createProperty(session, "name", YTType.STRING);
    cl.createIndex(session, "cl.name", INDEX_TYPE.NOTUNIQUE, "name");

    OSelectStatement stat = parseQuery("select from cl where name < 'a' and name > 'b'");
    OIndexFinder finder = new OClassIndexFinder("cl");
    BasicCommandContext ctx = new BasicCommandContext(session);

    Optional<OIndexCandidate> result = stat.getWhereClause().findIndex(finder, ctx);
    result = result.get().normalize(ctx);
    assertTrue((result.get() instanceof ORangeIndexCanditate));
    assertEquals("cl.name", result.get().getName());
    assertEquals(Operation.Range, result.get().getOperation());
  }

  private OSelectStatement parseQuery(String query) {
    InputStream is = new ByteArrayInputStream(query.getBytes());
    OrientSql osql = new OrientSql(is);
    try {
      SimpleNode n = osql.parse();
      return (OSelectStatement) n;
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
