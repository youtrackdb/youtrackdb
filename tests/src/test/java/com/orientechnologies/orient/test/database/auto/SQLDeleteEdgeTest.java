package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.OCommandExecutionException;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLSynchQuery;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @author Andrey Lomakin (a.lomakin-at-orientdb.com) <a
 * href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 * @since 04/12/14
 */
public class SQLDeleteEdgeTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SQLDeleteEdgeTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testDeleteFromTo() {
    database.command("CREATE CLASS testFromToOneE extends E").close();
    database.command("CREATE CLASS testFromToTwoE extends E").close();
    database.command("CREATE CLASS testFromToV extends V").close();

    database.begin();
    database.command("create vertex testFromToV set name = 'Luca'").close();
    database.command("create vertex testFromToV set name = 'Luca'").close();
    database.commit();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromToV"));

    database.begin();
    database
        .command(
            "CREATE EDGE testFromToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database.commit();

    OResultSet resultTwo =
        database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    database.begin();
    database
        .command(
            "DELETE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to"
                + result.get(0).getIdentity())
        .close();

    resultTwo = database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    database.command("DELETE FROM testFromToOneE unsafe").close();
    database.command("DELETE FROM testFromToTwoE unsafe").close();
    database.command("DELETE VERTEX testFromToV").close();
    database.commit();
  }

  public void testDeleteFrom() {
    database.command("CREATE CLASS testFromOneE extends E").close();
    database.command("CREATE CLASS testFromTwoE extends E").close();
    database.command("CREATE CLASS testFromV extends V").close();

    database.begin();
    database.command("create vertex testFromV set name = 'Luca'").close();
    database.command("create vertex testFromV set name = 'Luca'").close();
    database.commit();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testFromV"));

    database.begin();
    database
        .command(
            "CREATE EDGE testFromOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testFromTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database.commit();

    OResultSet resultTwo =
        database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    try {
      database.begin();
      database.command("DELETE EDGE testFromTwoE from " + result.get(1).getIdentity()).close();
      database.commit();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    resultTwo = database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    database.begin();
    database.command("DELETE FROM testFromOneE unsafe").close();
    database.command("DELETE FROM testFromTwoE unsafe").close();
    database.command("DELETE VERTEX testFromV").close();
    database.commit();
  }

  public void testDeleteTo() {
    database.command("CREATE CLASS testToOneE extends E").close();
    database.command("CREATE CLASS testToTwoE extends E").close();
    database.command("CREATE CLASS testToV extends V").close();

    database.begin();
    database.command("create vertex testToV set name = 'Luca'").close();
    database.command("create vertex testToV set name = 'Luca'").close();
    database.commit();

    List<OIdentifiable> result =
        database.query(new OSQLSynchQuery<ODocument>("select from testToV"));

    database.begin();
    database
        .command(
            "CREATE EDGE testToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database
        .command(
            "CREATE EDGE testToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    database.commit();

    OResultSet resultTwo =
        database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    database.begin();
    database.command("DELETE EDGE testToTwoE to " + result.get(0).getIdentity()).close();
    database.commit();

    resultTwo = database.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    database.begin();
    database.command("DELETE FROM testToOneE unsafe").close();
    database.command("DELETE FROM testToTwoE unsafe").close();
    database.command("DELETE VERTEX testToV").close();
    database.commit();
  }

  public void testDropClassVandEwithUnsafe() {
    database.command("CREATE CLASS SuperE extends E").close();
    database.command("CREATE CLASS SuperV extends V").close();

    database.begin();
    OIdentifiable v1 =
        database.command("create vertex SuperV set name = 'Luca'").next().getIdentity().get();
    OIdentifiable v2 =
        database.command("create vertex SuperV set name = 'Mark'").next().getIdentity().get();
    database
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    database.commit();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperV unsafe").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.fail();
    }

    try {
      database.command("DROP CLASS SuperE UNSAFE").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.fail();
    }
  }

  public void testDropClassVandEwithDeleteElements() {
    database.command("CREATE CLASS SuperE extends E").close();
    database.command("CREATE CLASS SuperV extends V").close();

    database.begin();
    OIdentifiable v1 =
        database.command("create vertex SuperV set name = 'Luca'").next().getIdentity().get();
    OIdentifiable v2 =
        database.command("create vertex SuperV set name = 'Mark'").next().getIdentity().get();
    database
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    database.commit();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.fail();
    } catch (OCommandExecutionException e) {
      Assert.assertTrue(true);
    }

    database.begin();
    database.command("DELETE VERTEX SuperV").close();
    database.commit();

    try {
      database.command("DROP CLASS SuperV").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.fail();
    }

    try {
      database.command("DROP CLASS SuperE").close();
      Assert.assertTrue(true);
    } catch (OCommandExecutionException e) {
      Assert.fail();
    }
  }

  public void testFromInString() {
    database.command("CREATE CLASS FromInStringE extends E").close();
    database.command("CREATE CLASS FromInStringV extends V").close();

    database.begin();
    OIdentifiable v1 =
        database
            .command("create vertex FromInStringV set name = ' from '")
            .next()
            .getIdentity()
            .get();
    OIdentifiable v2 =
        database
            .command("create vertex FromInStringV set name = ' FROM '")
            .next()
            .getIdentity()
            .get();
    OIdentifiable v3 =
        database
            .command("create vertex FromInStringV set name = ' TO '")
            .next()
            .getIdentity()
            .get();

    database
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    database
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v3.getIdentity())
        .close();
    database.commit();

    OResultSet result = database.query("SELECT expand(out()[name = ' FROM ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 1);

    result = database.query("SELECT expand(in()[name = ' from ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 2);

    result = database.query("SELECT expand(out()[name = ' TO ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void testDeleteVertexWithReturn() {
    database.begin();
    OIdentifiable v1 =
        database.command("create vertex V set returning = true").next().getIdentity().get();

    List<OIdentifiable> v2s =
        database.command("delete vertex V return before where returning = true").stream()
            .map((r) -> r.getIdentity().get())
            .collect(Collectors.toList());
    database.commit();

    Assert.assertEquals(v2s.size(), 1);
    Assert.assertTrue(v2s.contains(v1));
  }
}
