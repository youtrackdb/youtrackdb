package com.orientechnologies.orient.core.sql.functions.misc;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import org.junit.Assert;
import org.junit.Test;

public class OSQLFunctionIndexKeySizeTest extends DBTestBase {

  @Test
  public void test() {
    YTClass clazz = db.getMetadata().getSchema().createClass("Test");
    clazz.createProperty(db, "name", YTType.STRING);
    db.command("create index testindex on  Test (name) notunique").close();

    db.begin();
    db.command("insert into Test set name = 'a'").close();
    db.command("insert into Test set name = 'b'").close();
    db.commit();

    try (YTResultSet rs = db.query("select indexKeySize('testindex') as foo")) {
      Assert.assertTrue(rs.hasNext());
      YTResult item = rs.next();
      Assert.assertEquals((Object) 2L, item.getProperty("foo"));
      Assert.assertFalse(rs.hasNext());
    }
  }
}
