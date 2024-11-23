package com.orientechnologies.orient.core.sql.executor;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OType;
import java.util.List;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class OCreateLinkStatementExecutionTest extends BaseMemoryDatabase {

  @Test
  public void testBasic() {
    var basic1 = db.getMetadata().getSchema().createClass("Basic1");
    basic1.createProperty(db, "theLink", OType.LINK);

    var basic2 = db.getMetadata().getSchema().createClass("Basic2");
    basic2.createProperty(db, "theLink", OType.LINK);

    db.begin();
    db.command("insert into Basic1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Basic1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();

    db.command("insert into Basic2 set pk = 'pkb2_1'").close();
    db.command("insert into Basic2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink type link FROM Basic1.fk TO Basic2.pk ").close();
    db.commit();

    OResultSet result = db.query("select pk, theLink.pk as other from Basic1 order by pk");
    Assert.assertTrue(result.hasNext());

    OResult item = result.next();
    Object otherKey = item.getProperty("other");
    Assert.assertNotNull(otherKey);

    Assert.assertEquals(otherKey, "pkb2_1");

    Assert.assertTrue(result.hasNext());

    item = result.next();
    otherKey = item.getProperty("other");
    Assert.assertEquals(otherKey, "pkb2_2");
  }

  @Test
  public void testInverse() throws Exception {
    var inverse1 = db.getMetadata().getSchema().createClass("Inverse1");
    inverse1.createProperty(db, "theLink", OType.LINKSET);

    var inverse2 = db.getMetadata().getSchema().createClass("Inverse2");
    inverse2.createProperty(db, "theLink", OType.LINKSET);

    db.begin();
    db.command("insert into Inverse1 set pk = 'pkb1_1', fk = 'pkb2_1'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_2', fk = 'pkb2_2'").close();
    db.command("insert into Inverse1 set pk = 'pkb1_3', fk = 'pkb2_2'").close();

    db.command("insert into Inverse2 set pk = 'pkb2_1'").close();
    db.command("insert into Inverse2 set pk = 'pkb2_2'").close();

    db.command("CREATE LINK theLink TYPE LINKSET FROM Inverse1.fk TO Inverse2.pk INVERSE").close();
    db.commit();

    OResultSet result = db.query("select pk, theLink.pk as other from Inverse2 order by pk");
    Assert.assertTrue(result.hasNext());
    OResult item = result.next();

    Object otherKeys = item.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).get(0), "pkb1_1");

    Assert.assertTrue(result.hasNext());
    item = result.next();
    otherKeys = item.getProperty("other");
    Assert.assertNotNull(otherKeys);
    Assert.assertTrue(otherKeys instanceof List);
    Assert.assertEquals(((List) otherKeys).size(), 2);
    Assert.assertTrue(((List) otherKeys).contains("pkb1_2"));
    Assert.assertTrue(((List) otherKeys).contains("pkb1_3"));
  }
}
