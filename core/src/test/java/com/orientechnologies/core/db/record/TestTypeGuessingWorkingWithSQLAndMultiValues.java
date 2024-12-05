package com.orientechnologies.core.db.record;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResult;
import com.orientechnologies.core.sql.executor.YTResultSet;
import java.util.Collection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
public class TestTypeGuessingWorkingWithSQLAndMultiValues extends DBTestBase {

  @Before
  public void beforeTest() throws Exception {
    super.beforeTest();

    db.execute(
            "sql",
            """
                create class Address;
                create property Address.street String;
                create property Address.city String;
                create class Client;
                create property Client.name String;
                create property Client.phones embeddedSet String;
                create property Client.addresses embeddedList Address;""")
        .close();
  }

  @Test
  public void testLinkedValue() {

    try (YTResultSet result =
        db.execute(
            "sql",
            "begin; let res = insert into client set name = 'James Bond', phones = ['1234',"
                + " '34567'], addresses = [{'@class':'Address','city':'Shanghai', 'zip':'3999'},"
                + " {'@class':'Address','city':'New York', 'street':'57th Ave'}]\n"
                + ";update client set addresses = addresses ||"
                + " [{'@type':'d','@class':'Address','city':'London', 'zip':'67373'}]; commit;"
                + " return $res")) {
      Assert.assertTrue(result.hasNext());
      YTResult doc = result.next();

      Collection<YTEntityImpl> addresses = doc.getProperty("addresses");
      Assert.assertEquals(addresses.size(), 3);
      for (var a : addresses) {
        Assert.assertEquals("Address", a.getProperty("@class"));
      }
    }

    db.begin();
    try (YTResultSet result =
        db.command(
            "update client set addresses = addresses || [{'city':'London', 'zip':'67373'}] return"
                + " after")) {
      Assert.assertTrue(result.hasNext());

      YTResult doc = result.next();

      Collection<YTResult> addresses = doc.getProperty("addresses");
      Assert.assertEquals(addresses.size(), 4);

      for (YTResult a : addresses) {
        Assert.assertEquals("Address", a.getProperty("@class"));
      }
    }
    db.commit();
  }
}
