package com.orientechnologies.orient.core.db.graph;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.OCreateDatabaseUtil;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YouTrackDB;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class LightWeightEdgesTest {

  private YouTrackDB youTrackDB;
  private YTDatabaseSession session;

  @Before
  public void before() {
    youTrackDB =
        OCreateDatabaseUtil.createDatabase("test", DBTestBase.embeddedDBUrl(getClass()),
            OCreateDatabaseUtil.TYPE_MEMORY);
    session = youTrackDB.open("test", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD);

    session.createVertexClass("Vertex");
    session.createLightweightEdgeClass("Edge");
  }

  @Test
  public void testSimpleLightWeight() {
    session.begin();
    YTVertex v = session.newVertex("Vertex");
    YTVertex v1 = session.newVertex("Vertex");
    v.addLightWeightEdge(v1, "Edge");
    v.setProperty("name", "aName");
    v1.setProperty("name", "bName");
    session.save(v);
    session.commit();

    try (OResultSet res =
        session.query(" select expand(out('Edge')) from `Vertex` where name = 'aName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "bName");
    }

    try (OResultSet res =
        session.query(" select expand(in('Edge')) from `Vertex` where name = 'bName'")) {
      assertTrue(res.hasNext());
      OResult r = res.next();
      assertEquals(r.getProperty("name"), "aName");
    }
  }

  @Test
  public void testRegularBySchema() {
    String vClazz = "VtestRegularBySchema";
    YTClass vClass = session.createVertexClass(vClazz);

    String eClazz = "EtestRegularBySchema";
    YTClass eClass = session.createEdgeClass(eClazz);

    vClass.createProperty(session, "out_" + eClazz, YTType.LINKBAG, eClass);
    vClass.createProperty(session, "in_" + eClazz, YTType.LINKBAG, eClass);

    session.begin();
    YTVertex v = session.newVertex(vClass);
    v.setProperty("name", "a");
    v.save();
    YTVertex v1 = session.newVertex(vClass);
    v1.setProperty("name", "b");
    v1.save();
    session.commit();

    session.begin();
    session.command(
        "create edge "
            + eClazz
            + " from (select from "
            + vClazz
            + " where name = 'a') to (select from "
            + vClazz
            + " where name = 'b') set name = 'foo'");
    session.commit();

    session.execute(
        "sql",
        "begin;"
            + "delete edge "
            + eClazz
            + ";"
            + "create edge "
            + eClazz
            + " from (select from "
            + vClazz
            + " where name = 'a') to (select from "
            + vClazz
            + " where name = 'b') set name = 'foo';"
            + "commit;");
  }

  @After
  public void after() {
    session.close();
    youTrackDB.close();
  }
}
