package com.jetbrains.youtrack.db.internal.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class CommandExecutorSQLDeleteEdgeTest extends DbTestBase {

  private static RID folderId1;
  private static RID userId1;
  private List<Identifiable> edges;

  public void beforeTest() throws Exception {
    super.beforeTest();
    final Schema schema = session.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
    schema.createClass("Folder", schema.getClass("V"));
    schema.createClass("CanAccess", schema.getClass("E"));

    session.begin();
    var doc = ((EntityImpl) session.newEntity("User")).field("username", "gongolo");
    doc.save();
    userId1 = doc.getIdentity();
    doc = ((EntityImpl) session.newEntity("Folder")).field("keyId", "01234567893");
    doc.save();
    folderId1 = doc.getIdentity();
    session.commit();

    session.begin();
    edges =
        session.command("create edge CanAccess from " + userId1 + " to " + folderId1).stream()
            .map((x) -> x.getIdentity().get())
            .collect(Collectors.toList());
    session.commit();
  }

  @Test
  public void testFromSelect() {
    session.begin();
    var res =
        session.command(
            "delete edge CanAccess from (select from User where username = 'gongolo') to "
                + folderId1);
    session.commit();
    Assert.assertEquals((long) res.next().getProperty("count"), 1);
    Assert.assertFalse(session.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testFromSelectToSelect() {
    session.begin();
    var res =
        session.command(
            "delete edge CanAccess from ( select from User where username = 'gongolo' ) to ( select"
                + " from Folder where keyId = '01234567893' )");
    session.commit();

    assertEquals((long) res.next().getProperty("count"), 1);
    assertFalse(session.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testDeleteByRID() {
    session.begin();
    var result = session.command("delete edge [" + edges.get(0).getIdentity() + "]");
    session.commit();
    assertEquals((long) result.next().getProperty("count"), 1L);
  }

  @Test
  public void testDeleteEdgeWithVertexRid() {
    var vertexes = session.command("select from v limit 1");
    try {
      session.begin();
      session.command("delete edge [" + vertexes.next().getIdentity().get() + "]").close();
      session.commit();
      Assert.fail("Error on deleting an edge with a rid of a vertex");
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void testDeleteEdgeBatch() {
    // for issue #4622

    for (var i = 0; i < 100; i++) {
      session.begin();
      session.command("create vertex User set name = 'foo" + i + "'").close();
      session.command(
              "create edge CanAccess from (select from User where name = 'foo"
                  + i
                  + "') to "
                  + folderId1)
          .close();
      session.commit();
    }

    session.begin();
    session.command("delete edge CanAccess batch 5").close();
    session.commit();

    var result = session.query("select expand( in('CanAccess') ) from " + folderId1);
    assertEquals(result.stream().count(), 0);
  }
}
