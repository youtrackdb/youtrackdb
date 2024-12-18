package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.schema.Schema;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.query.Result;
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
public class SQLCreateVertexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public SQLCreateVertexTest(boolean remote) {
    super(remote);
  }

  public void testCreateVertexByContent() {
    db.close();

    db = createSessionInstance();

    Schema schema = db.getMetadata().getSchema();
    if (!schema.existsClass("CreateVertexByContent")) {
      SchemaClass vClass = schema.createClass("CreateVertexByContent", schema.getClass("V"));
      vClass.createProperty(db, "message", PropertyType.STRING);
    }

    db.begin();
    db.command("create vertex CreateVertexByContent content { \"message\": \"(:\"}").close();
    db
        .command(
            "create vertex CreateVertexByContent content { \"message\": \"\\\"‎ה, כן?...‎\\\"\"}")
        .close();
    db.commit();

    List<Result> result =
        db.query("select from CreateVertexByContent").stream().collect(Collectors.toList());
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
    db.close();
    db = createSessionInstance();

    db.begin();
    db.command("create vertex set script = true").close();
    db.command("create vertex").close();
    db.command("create vertex V").close();
    db.commit();

    // TODO complete this!
    // database.command(new CommandSQL("create vertex set")).execute();
    // database.command(new CommandSQL("create vertex set set set = 1")).execute();

  }

  public void testIsClassName() {
    db.close();

    db = createSessionInstance();
    db.createVertexClass("Like").createProperty(db, "anything", PropertyType.STRING);
    db.createVertexClass("Is").createProperty(db, "anything", PropertyType.STRING);
  }
}
