package com.orientechnologies.orient.console;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.impl.YTRecordBytes;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

/**
 *
 */
public class OConsoleDatabaseAppTest {

  @Rule
  public TestName testName = new TestName();

  @Test
  public void testSelectBinaryDoc() throws IOException {
    final StringBuilder builder = new StringBuilder();

    OConsoleDatabaseApp app =
        new OConsoleDatabaseApp(new String[]{}) {
          @Override
          public void message(String iMessage) {
            builder.append(iMessage).append("\n");
          }
        };
    try {

      app.executeServerCommand("connect env embedded:./target/ root root");
      app.executeServerCommand(
          "create database test memory users (admin identified by 'admin' role admin)");
      app.open("test", "admin", "admin");

      YTDatabaseSessionInternal db = (YTDatabaseSessionInternal) app.getCurrentDatabase();
      db.addBlobCluster("blobTest");

      db.begin();
      YTRecord record = db.save(new YTRecordBytes("blobContent".getBytes()), "blobTest");
      db.commit();
      builder.setLength(0);
      app.select(" from " + record.getIdentity() + " limit -1 ");
      assertTrue(builder.toString().contains("<binary>"));
    } finally {
      app.dropDatabase("memory");
    }
  }

  @Test
  public void testWrongCommand() {

    String builder =
        "connect env embedded:./target/ root root;\n"
            + "create database OConsoleDatabaseAppTest2 memory users (admin identified by 'admin'"
            + " role admin);\n"
            + "open OConsoleDatabaseAppTest2 admin admin;\n"
            + "create class foo;\n"
            + "begin;\n"
            + "insert into foo set name ="
            + " 'foo';\n"
            + "insert into foo set name ="
            + " 'bla';\n"
            + "blabla;\n" // <- wrong command, this should break the console
            + "update foo set surname = 'bar' where name = 'foo';\n"
            + "commit;\n";
    ConsoleTest c = new ConsoleTest(new String[]{builder});
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      var db = console.getCurrentDatabase();
      try {
        YTResultSet result = db.query("select from foo where name = 'foo'");
        YTResult doc = result.next();
        Assert.assertNull(doc.getProperty("surname"));
        Assert.assertFalse(result.hasNext());
      } finally {
        db.close();
      }
    } finally {
      console.close();
    }
  }

  @Test
  public void testOldCreateDatabase() {

    String builder =
        "create database memory:./target/OConsoleDatabaseAppTest2 admin adminpwd memory\n"
            + "create class foo;\n"
            + "begin;"
            + "insert into foo set name = 'foo';\n"
            + "insert into foo set name = 'bla';\n"
            + "commit;";
    ConsoleTest c = new ConsoleTest(new String[]{builder});
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      var db = console.getCurrentDatabase();
      try {
        long size = db.query("select from foo where name = 'foo'").stream().count();
        Assert.assertEquals(1, size);
      } finally {
        db.close();
      }
    } finally {
      console.close();
    }
  }

  @Test
  public void testDumpRecordDetails() {
    ConsoleTest c = new ConsoleTest();
    try {

      c.console().executeServerCommand("connect env embedded:./target/ root root");
      c.console()
          .executeServerCommand(
              "create database OConsoleDatabaseAppTestDumpRecordDetails memory users (admin"
                  + " identified by 'admin' role admin)");
      c.console().open("OConsoleDatabaseAppTestDumpRecordDetails", "admin", "admin");

      c.console().createClass("class foo");
      c.console().begin();
      c.console().insert("into foo set name = 'barbar'");
      c.console().commit();
      c.console().select("from foo limit -1");
      c.resetOutput();

      c.console().set("maxBinaryDisplay", "10000");
      c.console().displayRecord("0");

      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("@class:foo"));
      Assert.assertTrue(resultString.contains("barbar"));
    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testHelp() {
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().help(null);
      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("connect"));
      Assert.assertTrue(resultString.contains("alter class"));
      Assert.assertTrue(resultString.contains("create class"));
      Assert.assertTrue(resultString.contains("select"));
      Assert.assertTrue(resultString.contains("update"));
      Assert.assertTrue(resultString.contains("delete"));
      Assert.assertTrue(resultString.contains("create vertex"));
      Assert.assertTrue(resultString.contains("create edge"));
      Assert.assertTrue(resultString.contains("help"));
      Assert.assertTrue(resultString.contains("exit"));

    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testHelpCommand() {
    ConsoleTest c = new ConsoleTest();
    try {
      c.console().help("select");
      String resultString = c.getConsoleOutput();
      Assert.assertTrue(resultString.contains("COMMAND: select"));

    } catch (Exception e) {
      Assert.fail();
    } finally {
      c.shutdown();
    }
  }

  @Test
  public void testSimple() {

    String builder =
        "connect env embedded:./target/ root root;\n"
            + "create database "
            + testName.getMethodName()
            + " memory users (admin identified by 'admin' role admin);\n"
            + "open "
            + testName.getMethodName()
            + " admin admin;\n"
            + "profile storage on;\n"
            + "create class foo;\n"
            + "config;\n"
            + "list classes;\n"
            + "list properties;\n"
            + "list clusters;\n"
            + "list indexes;\n"
            + "info class OUser;\n"
            + "info property OUser.name;\n"
            + "begin;\n"
            + "insert into foo set name = 'foo';\n"
            + "insert into foo set name = 'bla';\n"
            + "update foo set surname = 'bar' where name = 'foo';\n"
            + "commit;\n"
            + "select from foo;\n"
            + "create class bar;\n"
            + "create property bar.name STRING;\n"
            + "create index bar_name on bar (name) NOTUNIQUE;\n"
            + "begin;\n"
            + "insert into bar set name = 'foo';\n"
            + "delete from bar;\n"
            + "commit;\n"
            + "begin;\n"
            + "insert into bar set name = 'foo';\n"
            + "rollback;\n"
            + "begin;\n"
            + "create vertex V set name = 'foo';\n"
            + "create vertex V set name = 'bar';\n"
            + "commit;\n"
            + "traverse out() from V;\n"
            + "begin;\n"
            + "create edge from (select from V where name = 'foo') to (select from V where name ="
            + " 'bar');\n"
            + "commit;\n"
            + "traverse out() from V;\n"
            + "profile storage off;\n"
            + "repair database -v;\n";
    ConsoleTest c = new ConsoleTest(new String[]{builder});
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      var db = console.getCurrentDatabase();
      YTResultSet result = db.query("select from foo where name = 'foo'");
      YTResult doc = result.next();
      Assert.assertEquals("bar", doc.getProperty("surname"));
      Assert.assertFalse(result.hasNext());
      result.close();

      result = db.query("select from bar");
      Assert.assertEquals(0, result.stream().count());

    } finally {
      console.close();
    }
  }

  @Test
  @Ignore
  public void testMultiLine() {
    String dbUrl = "memory:" + testName.getMethodName();

    String builder =
        "create database "
            + dbUrl
            + ";\n"
            + "profile storage on;\n"
            + "create class foo;\n"
            + "config;\n"
            + "list classes;\n"
            + "list properties;\n"
            + "list clusters;\n"
            + "list indexes;\n"
            + "info class OUser;\n"
            + "info property OUser.name;\n"
            + "begin;\n"
            + "insert into foo set name = 'foo';\n"
            + "insert into foo set name = 'bla';\n"
            + "update foo set surname = 'bar' where name = 'foo';\n"
            + "commit;\n"
            + "select from foo;\n"
            + "create class bar;\n"
            + "create property bar.name STRING;\n"
            + "create index bar_name on bar (name) NOTUNIQUE;\n"
            + "insert into bar set name = 'foo';\n"
            + "delete from bar;\n"
            + "begin;\n"
            + "insert into bar set name = 'foo';\n"
            + "rollback;\n"
            + "create vertex V set name = 'foo';\n"
            + "create vertex V set name = 'bar';\n"
            + "traverse out() from V;\n"

            //    builder.append("create edge from (select from V where name = 'foo') to (select
            // from V
            // where name = 'bar');\n");

            + "create edge from \n"
            + "(select from V where name = 'foo') \n"
            + "to (select from V where name = 'bar');\n";

    ConsoleTest c = new ConsoleTest(new String[]{builder});
    OConsoleDatabaseApp console = c.console();

    try {
      console.run();

      var db = console.getCurrentDatabase();
      YTResultSet result = db.query("select from foo where name = 'foo'");
      YTResult doc = result.next();
      Assert.assertEquals("bar", doc.getProperty("surname"));
      Assert.assertFalse(result.hasNext());
      result.close();

      result = db.query("select from bar");
      Assert.assertEquals(0, result.stream().count());

    } finally {
      console.close();
    }
  }

  class ConsoleTest {

    OConsoleDatabaseApp console;
    ByteArrayOutputStream out;
    PrintStream stream;

    ConsoleTest() {
      console =
          new OConsoleDatabaseApp(null) {
            @Override
            protected void onException(Throwable e) {
              super.onException(e);
              fail(e.getMessage());
            }
          };
      resetOutput();
    }

    ConsoleTest(String[] args) {
      console =
          new OConsoleDatabaseApp(args) {
            @Override
            protected void onException(Throwable e) {
              super.onException(e);
              fail(e.getMessage());
            }
          };
      resetOutput();
    }

    public OConsoleDatabaseApp console() {
      return console;
    }

    public String getConsoleOutput() {
      return out.toString();
    }

    void resetOutput() {
      if (out != null) {
        try {
          stream.close();
          out.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
      out = new ByteArrayOutputStream();
      stream = new PrintStream(out);
      console.setOutput(stream);
    }

    void shutdown() {
      if (out != null) {
        try {
          stream.close();
          out.close();
        } catch (Exception e) {
          e.printStackTrace();
        }
      }
      console.close();
    }
  }
}
