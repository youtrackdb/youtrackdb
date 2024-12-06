package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.metadata.schema.Schema;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.sql.executor.Result;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.testng.Assert;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 3/24/14
 */
@Test
public class SQLCreateVertexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public SQLCreateVertexTest(boolean remote) {
    super(remote);
  }

  public void testCreateVertexByContent() {
    database.close();

    database = createSessionInstance();

    Schema schema = database.getMetadata().getSchema();
    if (!schema.existsClass("CreateVertexByContent")) {
      SchemaClass vClass = schema.createClass("CreateVertexByContent", schema.getClass("V"));
      vClass.createProperty(database, "message", PropertyType.STRING);
    }

    database.begin();
    database.command("create vertex CreateVertexByContent content { \"message\": \"(:\"}").close();
    database
        .command(
            "create vertex CreateVertexByContent content { \"message\": \"\\\"‎ה, כן?...‎\\\"\"}")
        .close();
    database.commit();

    List<Result> result =
        database.query("select from CreateVertexByContent").stream().collect(Collectors.toList());
    Assert.assertEquals(result.size(), 2);

    List<String> messages = new ArrayList<String>();
    messages.add("\"‎ה, כן?...‎\"");
    messages.add("(:");

    List<String> resultMessages = new ArrayList<String>();

    for (Result document : result) {
      resultMessages.add(document.getProperty("message"));
    }

    //    issue #1787, works fine locally, not on CI
    Assert.assertEqualsNoOrder(
        messages.toArray(),
        resultMessages.toArray(),
        "arrays are different: " + toString(messages) + " - " + toString(resultMessages));
  }

  private String toString(List<String> resultMessages) {
    StringBuilder result = new StringBuilder();
    result.append("[");
    boolean first = true;
    for (String msg : resultMessages) {
      if (!first) {
        result.append(", ");
      }
      result.append("\"");
      result.append(msg);
      result.append("\"");
      first = false;
    }
    result.append("]");
    return result.toString();
  }

  public void testCreateVertexBooleanProp() {
    database.close();
    database = createSessionInstance();

    database.begin();
    database.command("create vertex set script = true").close();
    database.command("create vertex").close();
    database.command("create vertex V").close();
    database.commit();

    // TODO complete this!
    // database.command(new CommandSQL("create vertex set")).execute();
    // database.command(new CommandSQL("create vertex set set set = 1")).execute();

  }

  public void testIsClassName() {
    database.close();

    database = createSessionInstance();
    database.createVertexClass("Like").createProperty(database, "anything", PropertyType.STRING);
    database.createVertexClass("Is").createProperty(database, "anything", PropertyType.STRING);
  }
}
