package com.orientechnologies.orient.core.sql.executor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.metadata.schema.YTSchema;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
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
public class OCommandExecutorSQLDeleteEdgeTest extends DBTestBase {

  private static YTRID folderId1;
  private static YTRID userId1;
  private List<YTIdentifiable> edges;

  public void beforeTest() throws Exception {
    super.beforeTest();
    final YTSchema schema = db.getMetadata().getSchema();
    schema.createClass("User", schema.getClass("V"));
    schema.createClass("Folder", schema.getClass("V"));
    schema.createClass("CanAccess", schema.getClass("E"));

    db.begin();
    var doc = new YTEntityImpl("User").field("username", "gongolo");
    doc.save();
    userId1 = doc.getIdentity();
    doc = new YTEntityImpl("Folder").field("keyId", "01234567893");
    doc.save();
    folderId1 = doc.getIdentity();
    db.commit();

    db.begin();
    edges =
        db.command("create edge CanAccess from " + userId1 + " to " + folderId1).stream()
            .map((x) -> x.getIdentity().get())
            .collect(Collectors.toList());
    db.commit();
  }

  @Test
  public void testFromSelect() {
    db.begin();
    YTResultSet res =
        db.command(
            "delete edge CanAccess from (select from User where username = 'gongolo') to "
                + folderId1);
    db.commit();
    Assert.assertEquals((long) res.next().getProperty("count"), 1);
    Assert.assertFalse(db.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testFromSelectToSelect() {
    db.begin();
    YTResultSet res =
        db.command(
            "delete edge CanAccess from ( select from User where username = 'gongolo' ) to ( select"
                + " from Folder where keyId = '01234567893' )");
    db.commit();

    assertEquals((long) res.next().getProperty("count"), 1);
    assertFalse(db.query("select expand(out(CanAccess)) from " + userId1).hasNext());
  }

  @Test
  public void testDeleteByRID() {
    db.begin();
    YTResultSet result = db.command("delete edge [" + edges.get(0).getIdentity() + "]");
    db.commit();
    assertEquals((long) result.next().getProperty("count"), 1L);
  }

  @Test
  public void testDeleteEdgeWithVertexRid() {
    YTResultSet vertexes = db.command("select from v limit 1");
    try {
      db.begin();
      db.command("delete edge [" + vertexes.next().getIdentity().get() + "]").close();
      db.commit();
      Assert.fail("Error on deleting an edge with a rid of a vertex");
    } catch (Exception e) {
      // OK
    }
  }

  @Test
  public void testDeleteEdgeBatch() {
    // for issue #4622

    for (int i = 0; i < 100; i++) {
      db.begin();
      db.command("create vertex User set name = 'foo" + i + "'").close();
      db.command(
              "create edge CanAccess from (select from User where name = 'foo"
                  + i
                  + "') to "
                  + folderId1)
          .close();
      db.commit();
    }

    db.begin();
    db.command("delete edge CanAccess batch 5").close();
    db.commit();

    YTResultSet result = db.query("select expand( in('CanAccess') ) from " + folderId1);
    assertEquals(result.stream().count(), 0);
  }
}
