package com.jetbrains.youtrack.db.internal.core.sql.executor;

import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.HashMap;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/**
 *
 */
@RunWith(JUnit4.class)
public class CommandExecutorSQLCreateEdgeTest extends DbTestBase {

  private EntityImpl owner1;
  private EntityImpl owner2;

  public void beforeTest() throws Exception {
    super.beforeTest();

    final Schema schema = session.getMetadata().getSchema();
    schema.createClass("Owner", schema.getClass("V"));
    schema.createClass("link", schema.getClass("E"));

    session.begin();
    owner1 = (EntityImpl) session.newEntity("Owner");
    owner1.field("id", 1);

    owner2 = (EntityImpl) session.newEntity("Owner");
    owner2.field("id", 2);

    session.commit();
  }

  @Test
  public void testParametersBinding() {
    session.begin();
    session.command(
            "CREATE EDGE link from "
                + owner1.getIdentity()
                + " TO "
                + owner2.getIdentity()
                + " SET foo = ?",
            "123")
        .close();
    session.commit();

    var list = session.query("SELECT FROM link");

    var res = list.next();
    Assert.assertEquals(res.getProperty("foo"), "123");
    Assert.assertFalse(list.hasNext());
  }

  @Test
  public void testSubqueryParametersBinding() throws Exception {
    final var params = new HashMap<String, Object>();
    params.put("foo", "bar");
    params.put("fromId", 1);
    params.put("toId", 2);

    session.begin();
    session.command(
            "CREATE EDGE link from (select from Owner where id = :fromId) TO (select from Owner"
                + " where id = :toId) SET foo = :foo",
            params)
        .close();
    session.commit();

    var list = session.query("SELECT FROM link");

    var edge = list.next();
    Assert.assertEquals(edge.getProperty("foo"), "bar");
    Assert.assertEquals(edge.getProperty("out"), owner1.getIdentity());
    Assert.assertEquals(edge.getProperty("in"), owner2.getIdentity());
    Assert.assertFalse(list.hasNext());
  }

  @Test
  public void testBatch() throws Exception {
    for (var i = 0; i < 20; ++i) {
      session.begin();
      session.command("CREATE VERTEX Owner SET testbatch = true, id = ?", i).close();
      session.commit();
    }

    session.begin();
    var edges =
        session.command(
            "CREATE EDGE link from (select from owner where testbatch = true and id > 0) TO (select"
                + " from owner where testbatch = true and id = 0) batch 10",
            "456");
    session.commit();

    Assert.assertEquals(edges.stream().count(), 19);

    var list = session.query("select from owner where testbatch = true and id = 0");

    var res = list.next();
    Assert.assertEquals(((RidBag) res.getProperty("in_link")).size(), 19);
    Assert.assertFalse(list.hasNext());
  }

  @Test
  public void testEdgeConstraints() {
    session.execute(
            "sql",
            "create class E2 extends E;"
                + "create property E2.x LONG;"
                + "create property E2.in LINK;"
                + "alter property E2.in MANDATORY true;"
                + "create property E2.out LINK;"
                + "alter property E2.out MANDATORY true;"
                + "create class E1 extends E;"
                + "create property E1.x LONG;"
                + "alter property E1.x MANDATORY true;"
                + "create property E1.in LINK;"
                + "alter property E1.in MANDATORY true;"
                + "create property E1.out LINK;"
                + "alter property E1.out MANDATORY true;"
                + "create class FooType extends V;"
                + "create property FooType.name STRING;"
                + "alter property FooType.name MANDATORY true;")
        .close();

    session.execute(
            "sql",
            "begin;"
                + "let $v1 = create vertex FooType content {'name':'foo1'};"
                + "let $v2 = create vertex FooType content {'name':'foo2'};"
                + "create edge E1 from $v1 to $v2 content {'x':22};"
                + "create edge E1 from $v1 to $v2 set x=22;"
                + "create edge E2 from $v1 to $v2 content {'x':345};"
                + "commit;")
        .close();
  }
}
