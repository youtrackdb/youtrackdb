package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLSynchQuery;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * href="mailto:lomakin.andrey@gmail.com">Andrey Lomakin</a>
 *
 * @since 04/12/14
 */
public class SQLDeleteEdgeTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLDeleteEdgeTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  public void testDeleteFromTo() {
    db.command("CREATE CLASS testFromToOneE extends E").close();
    db.command("CREATE CLASS testFromToTwoE extends E").close();
    db.command("CREATE CLASS testFromToV extends V").close();

    db.begin();
    db.command("create vertex testFromToV set name = 'Luca'").close();
    db.command("create vertex testFromToV set name = 'Luca'").close();
    db.commit();

    List<Identifiable> result =
        db.query(new SQLSynchQuery<EntityImpl>("select from testFromToV"));

    db.begin();
    db
        .command(
            "CREATE EDGE testFromToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db
        .command(
            "CREATE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db.commit();

    ResultSet resultTwo =
        db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    db.begin();
    db
        .command(
            "DELETE EDGE testFromToTwoE from "
                + result.get(1).getIdentity()
                + " to"
                + result.get(0).getIdentity())
        .close();

    resultTwo = db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    db.command("DELETE FROM testFromToOneE unsafe").close();
    db.command("DELETE FROM testFromToTwoE unsafe").close();
    db.command("DELETE VERTEX testFromToV").close();
    db.commit();
  }

  public void testDeleteFrom() {
    db.command("CREATE CLASS testFromOneE extends E").close();
    db.command("CREATE CLASS testFromTwoE extends E").close();
    db.command("CREATE CLASS testFromV extends V").close();

    db.begin();
    db.command("create vertex testFromV set name = 'Luca'").close();
    db.command("create vertex testFromV set name = 'Luca'").close();
    db.commit();

    List<Identifiable> result =
        db.query(new SQLSynchQuery<EntityImpl>("select from testFromV"));

    db.begin();
    db
        .command(
            "CREATE EDGE testFromOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db
        .command(
            "CREATE EDGE testFromTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db.commit();

    ResultSet resultTwo =
        db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    try {
      db.begin();
      db.command("DELETE EDGE testFromTwoE from " + result.get(1).getIdentity()).close();
      db.commit();
    } catch (Exception e) {
      e.printStackTrace();
      throw e;
    }

    resultTwo = db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    db.begin();
    db.command("DELETE FROM testFromOneE unsafe").close();
    db.command("DELETE FROM testFromTwoE unsafe").close();
    db.command("DELETE VERTEX testFromV").close();
    db.commit();
  }

  public void testDeleteTo() {
    db.command("CREATE CLASS testToOneE extends E").close();
    db.command("CREATE CLASS testToTwoE extends E").close();
    db.command("CREATE CLASS testToV extends V").close();

    db.begin();
    db.command("create vertex testToV set name = 'Luca'").close();
    db.command("create vertex testToV set name = 'Luca'").close();
    db.commit();

    List<Identifiable> result =
        db.query(new SQLSynchQuery<EntityImpl>("select from testToV"));

    db.begin();
    db
        .command(
            "CREATE EDGE testToOneE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db
        .command(
            "CREATE EDGE testToTwoE from "
                + result.get(1).getIdentity()
                + " to "
                + result.get(0).getIdentity())
        .close();
    db.commit();

    ResultSet resultTwo =
        db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 2);

    db.begin();
    db.command("DELETE EDGE testToTwoE to " + result.get(0).getIdentity()).close();
    db.commit();

    resultTwo = db.query("select expand(outE()) from " + result.get(1).getIdentity());

    Assert.assertEquals(resultTwo.stream().count(), 1);

    db.begin();
    db.command("DELETE FROM testToOneE unsafe").close();
    db.command("DELETE FROM testToTwoE unsafe").close();
    db.command("DELETE VERTEX testToV").close();
    db.commit();
  }

  public void testDropClassVandEwithUnsafe() {
    db.command("CREATE CLASS SuperE extends E").close();
    db.command("CREATE CLASS SuperV extends V").close();

    db.begin();
    Identifiable v1 =
        db.command("create vertex SuperV set name = 'Luca'").next().getIdentity().get();
    Identifiable v2 =
        db.command("create vertex SuperV set name = 'Mark'").next().getIdentity().get();
    db
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    db.commit();

    try {
      db.command("DROP CLASS SuperV").close();
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      db.command("DROP CLASS SuperE").close();
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      db.command("DROP CLASS SuperV unsafe").close();
      Assert.assertTrue(true);
    } catch (CommandExecutionException e) {
      Assert.fail();
    }

    try {
      db.command("DROP CLASS SuperE UNSAFE").close();
      Assert.assertTrue(true);
    } catch (CommandExecutionException e) {
      Assert.fail();
    }
  }

  public void testDropClassVandEwithDeleteElements() {
    db.command("CREATE CLASS SuperE extends E").close();
    db.command("CREATE CLASS SuperV extends V").close();

    db.begin();
    Identifiable v1 =
        db.command("create vertex SuperV set name = 'Luca'").next().getIdentity().get();
    Identifiable v2 =
        db.command("create vertex SuperV set name = 'Mark'").next().getIdentity().get();
    db
        .command("CREATE EDGE SuperE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    db.commit();

    try {
      db.command("DROP CLASS SuperV").close();
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }

    try {
      db.command("DROP CLASS SuperE").close();
      Assert.fail();
    } catch (CommandExecutionException e) {
      Assert.assertTrue(true);
    }

    db.begin();
    db.command("DELETE VERTEX SuperV").close();
    db.commit();

    try {
      db.command("DROP CLASS SuperV").close();
      Assert.assertTrue(true);
    } catch (CommandExecutionException e) {
      Assert.fail();
    }

    try {
      db.command("DROP CLASS SuperE").close();
      Assert.assertTrue(true);
    } catch (CommandExecutionException e) {
      Assert.fail();
    }
  }

  public void testFromInString() {
    db.command("CREATE CLASS FromInStringE extends E").close();
    db.command("CREATE CLASS FromInStringV extends V").close();

    db.begin();
    Identifiable v1 =
        db
            .command("create vertex FromInStringV set name = ' from '")
            .next()
            .getIdentity()
            .get();
    Identifiable v2 =
        db
            .command("create vertex FromInStringV set name = ' FROM '")
            .next()
            .getIdentity()
            .get();
    Identifiable v3 =
        db
            .command("create vertex FromInStringV set name = ' TO '")
            .next()
            .getIdentity()
            .get();

    db
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v2.getIdentity())
        .close();
    db
        .command("create edge FromInStringE from " + v1.getIdentity() + " to " + v3.getIdentity())
        .close();
    db.commit();

    ResultSet result = db.query("SELECT expand(out()[name = ' FROM ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 1);

    result = db.query("SELECT expand(in()[name = ' from ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 2);

    result = db.query("SELECT expand(out()[name = ' TO ']) FROM FromInStringV");
    Assert.assertEquals(result.stream().count(), 1);
  }

  public void testDeleteVertexWithReturn() {
    db.begin();
    Identifiable v1 =
        db.command("create vertex V set returning = true").next().getIdentity().get();

    List<Identifiable> v2s =
        db.command("delete vertex V return before where returning = true").stream()
            .map((r) -> r.getIdentity().get())
            .collect(Collectors.toList());
    db.commit();

    Assert.assertEquals(v2s.size(), 1);
    Assert.assertTrue(v2s.contains(v1));
  }
}
