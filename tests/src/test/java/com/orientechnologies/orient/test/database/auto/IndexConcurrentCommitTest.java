package com.orientechnologies.orient.test.database.auto;

import com.orientechnologies.orient.core.index.YTIndexException;
import com.orientechnologies.orient.core.metadata.schema.YTClass;
import com.orientechnologies.orient.core.metadata.schema.YTType;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexConcurrentCommitTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public IndexConcurrentCommitTest(boolean remote) {
    super(remote);
  }

  public void testConcurrentUpdate() {
    YTClass personClass = database.getMetadata().getSchema().createClass("Person");
    personClass.createProperty(database, "ssn", YTType.STRING)
        .createIndex(database, YTClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty(database, "name", YTType.STRING)
        .createIndex(database, YTClass.INDEX_TYPE.NOTUNIQUE);

    try {
      // Transaction 1
      database.begin();

      // Insert two people in a transaction
      YTDocument person1 = new YTDocument("Person");
      person1.field("name", "John Doe");
      person1.field("ssn", "111-11-1111");
      person1.save();

      YTDocument person2 = new YTDocument("Person");
      person2.field("name", "Jane Doe");
      person2.field("ssn", "222-22-2222");
      person2.save();

      // Commit
      database.commit();

      // Ensure that the people made it in correctly
      final YTResultSet result1 = database.query("select from Person");
      while (result1.hasNext()) {
        System.out.println(result1.next());
      }

      // Transaction 2
      database.begin();

      // Update the ssn for the second person
      person2.field("ssn", "111-11-1111");
      person2.save();

      // Update the ssn for the first person
      person1.field("ssn", "222-22-2222");
      person1.save();

      System.out.println("To be committed:");
      System.out.println(person1);
      System.out.println(person2);
      // Commit - We get a transaction failure!
      database.commit();

      System.out.println("Success!");
    } catch (YTIndexException e) {
      System.out.println("Exception: " + e);
      database.rollback();
    }

    final YTResultSet result2 = database.command("select from Person");
    System.out.println("After transaction 2");
    while (result2.hasNext()) {
      System.out.println(result2.next());
    }
  }
}
