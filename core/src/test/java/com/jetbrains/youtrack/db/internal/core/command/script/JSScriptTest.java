package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.common.io.IOUtils;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;
import javax.script.ScriptException;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class JSScriptTest extends DbTestBase {

  @Test
  public void jsSimpleTest() {
    var resultSet = db.execute("javascript", "'foo'");
    Assert.assertTrue(resultSet.hasNext());
    var result = resultSet.next();
    String ret = result.getProperty("value");
    Assert.assertEquals("foo", ret);
  }

  @Test
  public void jsQueryTest() {
    var script = "db.query('select from OUser')";
    var resultSet = db.execute("javascript", script);
    Assert.assertTrue(resultSet.hasNext());

    var results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(2, results.size()); // no default users anymore, 'admin' created

    results.stream()
        .map(r -> r.getEntity().get())
        .forEach(
            oElement -> {
              Assert.assertEquals("OUser", oElement.getSchemaType().get().getName());
            });

  }

  @Test
  public void jsScriptTest() throws IOException {
    var stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptTest.js");
    var resultSet = db.execute("javascript", IOUtils.readStreamAsString(stream));
    Assert.assertTrue(resultSet.hasNext());

    var results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(1, results.size());

    var value = results.get(0).getProperty("value");
    var values = (Collection<Result>) value;
    values.stream()
        .map(r -> r.getEntity().get())
        .forEach(
            oElement -> {
              Assert.assertEquals("OUser", oElement.getSchemaType().get().getName());
            });

  }

  @Test
  public void jsScriptCountTest() throws IOException {
    var stream = ClassLoader.getSystemResourceAsStream("fixtures/scriptCountTest.js");
    var resultSet = db.execute("javascript", IOUtils.readStreamAsString(stream));
    Assert.assertTrue(resultSet.hasNext());

    var results = resultSet.stream().collect(Collectors.toList());
    Assert.assertEquals(1, results.size());

    Number value = results.get(0).getProperty("value");
    Assert.assertEquals(2, value.intValue()); // no default users anymore, 'admin' created
  }

  @Test
  public void jsSandboxTestWithJavaType() {
    try {
      final var result =
          db.execute(
              "javascript", "var File = Java.type(\"java.io.File\");\n  File.pathSeparator;");

      Assert.fail("It should receive a class not found exception");
    } catch (RuntimeException e) {
      Assert.assertEquals(
          GlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
              ? ScriptException.class
              : ClassNotFoundException.class,
          e.getCause().getClass());
    }
  }

  // @Test
  // THIS TEST WONT PASS WITH GRAALVM
  public void jsSandboxWithNativeTest() {
    var scriptManager = YouTrackDBInternal.extract(context).getScriptManager();
    try {
      scriptManager.addAllowedPackages(new HashSet<>(List.of("java.lang.System")));

      var resultSet =
          db.execute(
              "javascript", "var System = Java.type('java.lang.System'); System.nanoTime();");
      Assert.assertEquals(0, resultSet.stream().count());
    } finally {
      scriptManager.removeAllowedPackages(new HashSet<>(List.of("java.lang.System")));
    }
  }

  @Test
  public void jsSandboxWithMathTest() {
    var resultSet = db.execute("javascript", "Math.random()");
    Assert.assertEquals(1, resultSet.stream().count());
    resultSet.close();
  }

  @Test
  public void jsSandboxWithDB() {
    var resultSet =
        db.execute(
            "javascript",
            """
                var rs = db.query("select from OUser");
                var elem = rs.next();
                var prop = elem.getProperty("name");
                rs.close();
                prop;
                """);
    Assert.assertEquals(1, resultSet.stream().count());
    resultSet.close();
  }

  @Test
  public void jsSandboxWithBigDecimal() {
    final var scriptManager = YouTrackDBInternal.extract(context).getScriptManager();
    try {
      scriptManager.addAllowedPackages(new HashSet<>(List.of("java.math.BigDecimal")));

      try (var resultSet =
          db.execute(
              "javascript",
              "var BigDecimal = Java.type('java.math.BigDecimal'); new BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }
      scriptManager.removeAllowedPackages(new HashSet<>(List.of("java.math.BigDecimal")));
      scriptManager.closeAll();

      try {
        db.execute("javascript", "new java.math.BigDecimal(1.0);");
        Assert.fail("It should receive a class not found exception");
      } catch (RuntimeException e) {
        Assert.assertEquals(
            GlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
                ? ScriptException.class
                : ClassNotFoundException.class,
            e.getCause().getClass());
      }

      scriptManager.addAllowedPackages(new HashSet<>(List.of("java.math.*")));
      scriptManager.closeAll();

      try (var resultSet = db.execute("javascript", "new java.math.BigDecimal(1.0);")) {
        Assert.assertEquals(1, resultSet.stream().count());
      }

    } finally {
      scriptManager.removeAllowedPackages(
          new HashSet<>(Arrays.asList("java.math.BigDecimal", "java.math.*")));
    }
  }

  @Test
  public void jsSandboxWithYouTrackDb() {
    try (var resultSet =
        db.execute("javascript", "youtrackdb.getScriptManager().addAllowedPackages([])")) {
      Assert.assertEquals(1, resultSet.stream().count());
    } catch (Exception e) {
      Assert.assertEquals(ScriptException.class, e.getCause().getClass());
    }

    try (var resultSet =
        db.execute(
            "javascript",
            "youtrackdb.getScriptManager().addAllowedPackages([])")) {
      Assert.assertEquals(1, resultSet.stream().count());
    } catch (Exception e) {
      Assert.assertEquals(
          GlobalConfiguration.SCRIPT_POLYGLOT_USE_GRAAL.getValueAsBoolean()
              ? ScriptException.class
              : ClassNotFoundException.class,
          e.getCause().getClass());
    }
  }
}
