package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import static org.junit.Assert.assertEquals;

import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import org.junit.Test;

public class CreateClassMultipleClusterTest extends DbTestBase {

  @Test
  public void testCreateClassSQL() {
    db.command("create class CCTest clusters 16").close();
    db.command("create class X extends CCTest clusters 32").close();

    final var clazzV = db.getMetadata().getSchema().getClass("CCTest");
    assertEquals(16, clazzV.getClusterIds().length);

    final var clazzX = db.getMetadata().getSchema().getClass("X");
    assertEquals(32, clazzX.getClusterIds().length);
  }

  @Test
  public void testCreateClassSQLSpecifiedClusters() {
    var s = db.addCluster("second");
    var t = db.addCluster("third");
    db.command("create class CCTest2 cluster " + s + "," + t).close();

    final var clazzV = db.getMetadata().getSchema().getClass("CCTest2");
    assertEquals(2, clazzV.getClusterIds().length);

    assertEquals(s, clazzV.getClusterIds()[0]);
    assertEquals(t, clazzV.getClusterIds()[1]);
  }
}
