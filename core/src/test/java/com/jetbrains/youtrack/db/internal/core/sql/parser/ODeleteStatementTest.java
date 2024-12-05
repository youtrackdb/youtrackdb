package com.jetbrains.youtrack.db.internal.core.sql.parser;

import static org.junit.Assert.fail;

import com.jetbrains.youtrack.db.internal.DBTestBase;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResult;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

public class ODeleteStatementTest extends DBTestBase {

  protected SimpleNode checkRightSyntax(String query) {
    return checkSyntax(query, true);
  }

  protected SimpleNode checkWrongSyntax(String query) {
    return checkSyntax(query, false);
  }

  protected SimpleNode checkSyntax(String query, boolean isCorrect) {
    OrientSql osql = getParserFor(query);
    try {
      SimpleNode result = osql.parse();
      if (!isCorrect) {
        fail();
      }
      return result;
    } catch (Exception e) {
      if (isCorrect) {
        e.printStackTrace();
        fail();
      }
    }
    return null;
  }

  @Test
  public void deleteFromSubqueryWithWhereTest() {

    db.command("create class Foo").close();
    db.command("create class Bar").close();

    db.begin();
    final EntityImpl doc1 = new EntityImpl("Foo").field("k", "key1");
    final EntityImpl doc2 = new EntityImpl("Foo").field("k", "key2");
    final EntityImpl doc3 = new EntityImpl("Foo").field("k", "key3");

    doc1.save();
    doc2.save();
    doc3.save();

    List<EntityImpl> list = new ArrayList<EntityImpl>();
    list.add(doc1);
    list.add(doc2);
    list.add(doc3);
    final EntityImpl bar = new EntityImpl("Bar").field("arr", list);
    bar.save();
    db.commit();

    db.begin();
    db.command("delete from (select expand(arr) from Bar) where k = 'key2'").close();
    db.commit();

    try (YTResultSet result = db.query("select from Foo")) {
      Assert.assertNotNull(result);
      int count = 0;
      while (result.hasNext()) {
        YTResult doc = result.next();
        Assert.assertNotEquals(doc.getProperty("k"), "key2");
        count += 1;
      }
      Assert.assertEquals(count, 2);
    }
  }

  protected OrientSql getParserFor(String string) {
    InputStream is = new ByteArrayInputStream(string.getBytes());
    OrientSql osql = new OrientSql(is);
    return osql;
  }
}
