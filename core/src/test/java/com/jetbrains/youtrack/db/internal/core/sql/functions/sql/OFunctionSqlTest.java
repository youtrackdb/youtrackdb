package com.jetbrains.youtrack.db.internal.core.sql.functions.sql;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.command.BasicCommandContext;
import com.jetbrains.youtrack.db.internal.core.metadata.function.OFunction;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.OLegacyResultSet;
import java.util.ArrayList;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OFunctionSqlTest extends DBTestBase {

  @Test
  public void functionSqlWithParameters() {
    db.getMetadata().getSchema().createClass("Test");

    db.begin();
    EntityImpl doc1 = new EntityImpl("Test");
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1 = new EntityImpl("Test");
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);
    db.commit();

    db.begin();
    OFunction function = new OFunction(db);
    function.setName(db, "test");
    function.setCode(db, "select from Test where name = :name");
    function.setParameters(db,
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

    Assert.assertEquals(1, ((OLegacyResultSet<?>) result).size());
  }

  @Test
  public void functionSqlWithInnerFunctionJs() {

    db.getMetadata().getSchema().createClass("Test");
    db.begin();
    EntityImpl doc1 = new EntityImpl("Test");
    doc1.field("name", "Enrico");
    db.save(doc1);
    doc1 = new EntityImpl("Test");
    doc1.setClassName("Test");
    doc1.field("name", "Luca");
    db.save(doc1);
    db.commit();

    db.begin();
    OFunction function = new OFunction(db);
    function.setName(db, "test");
    function.setCode(db,
        "select name from Test where name = :name and hello(:name) = 'Hello Enrico'");
    function.setParameters(db,
        new ArrayList<>() {
          {
            add("name");
          }
        });
    function.save(db);
    db.commit();

    db.begin();
    OFunction function1 = new OFunction(db);
    function1.setName(db, "hello");
    function1.setLanguage(db, "javascript");
    function1.setCode(db, "return 'Hello ' + name");
    function1.setParameters(db,
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
    Assert.assertEquals(1, ((OLegacyResultSet) result).size());
  }
}
