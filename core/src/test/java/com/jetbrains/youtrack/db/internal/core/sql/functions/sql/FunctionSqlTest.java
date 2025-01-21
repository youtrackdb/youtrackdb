package com.jetbrains.youtrack.db.internal.core.sql.functions.sql;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.function.Function;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.LegacyResultSet;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class FunctionSqlTest extends DbTestBase {

  @Test
  public void functionSqlWithParameters() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    EntityImpl doc1 = ((EntityImpl) db.newEntity("Test"));
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1 = ((EntityImpl) db.newEntity("Test"));
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);
    db.commit();

    db.begin();
    Function function = new Function(db);
    function.setName("test");
    function.setCode("select from Test where name = :name");
    function.setParameters(
        new ArrayList<>() {
          {
            add("name");
          }
        });
    function.save(db);
    db.commit();

    var context = new BasicCommandContext();
    context.setDatabase(db);

    Object result = function.executeInContext(context, "Enrico");

    Assert.assertEquals(1, ((LegacyResultSet<?>) result).size());
  }

  @Test
  public void functionSqlWithInnerFunctionJs() {

    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    EntityImpl doc1 = ((EntityImpl) db.newEntity("Test"));
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1 = ((EntityImpl) db.newEntity("Test"));
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);
    db.commit();

    db.begin();
    Function function = new Function(db);
    function.setName("test");
    function.setCode(
        "select name from Test where name = :name and hello(:name) = 'Hello Enrico'");
    function.setParameters(
        new ArrayList<>() {
          {
            add("name");
          }
        });
    function.save(db);
    db.commit();

    db.begin();
    Function function1 = new Function(db);
    function1.setName("hello");
    function1.setLanguage("javascript");
    function1.setCode("return 'Hello ' + name");
    function1.setParameters(
        new ArrayList<>() {
          {
            add("name");
          }
        });
    function1.save(db);
    db.commit();

    var context = new BasicCommandContext();
    context.setDatabase(db);

    Object result = function.executeInContext(context, "Enrico");
    Assert.assertEquals(1, ((LegacyResultSet) result).size());
  }
}
