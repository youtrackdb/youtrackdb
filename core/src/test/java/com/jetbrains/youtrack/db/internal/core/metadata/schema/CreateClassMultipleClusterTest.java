package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

public class CreateClassMultipleClusterTest extends DbTestBase {

  @Test
  public void testCreateClassSQL() {
    session.command("create class CCTest clusters 16").close();
    session.command("create class X extends CCTest clusters 32").close();

    final var clazzV = session.getMetadata().getSchema().getClass("CCTest");
    assertEquals(16, clazzV.getClusterIds(session).length);

    final var clazzX = session.getMetadata().getSchema().getClass("X");
    assertEquals(32, clazzX.getClusterIds(session).length);
  }

  @Test
  public void testCreateClassSQLSpecifiedClusters() {
    var s = session.addCluster("second");
    var t = session.addCluster("third");
    session.command("create class CCTest2 cluster " + s + "," + t).close();

    final var clazzV = session.getMetadata().getSchema().getClass("CCTest2");
    assertEquals(2, clazzV.getClusterIds(session).length);

    assertEquals(s, clazzV.getClusterIds(session)[0]);
    assertEquals(t, clazzV.getClusterIds(session)[1]);
  }
}
