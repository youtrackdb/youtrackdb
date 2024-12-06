package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexConcurrentCommitTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public IndexConcurrentCommitTest(boolean remote) {
    super(remote);
  }

  public void testConcurrentUpdate() {
    SchemaClass personClass = database.getMetadata().getSchema().createClass("Person");
    personClass.createProperty(database, "ssn", PropertyType.STRING)
        .createIndex(database, SchemaClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty(database, "name", PropertyType.STRING)
        .createIndex(database, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    try {
      // Transaction 1
      database.begin();

      // Insert two people in a transaction
      EntityImpl person1 = new EntityImpl("Person");
      person1.field("name", "John Doe");
      person1.field("ssn", "111-11-1111");
      person1.save();

      EntityImpl person2 = new EntityImpl("Person");
      person2.field("name", "Jane Doe");
      person2.field("ssn", "222-22-2222");
      person2.save();

      // Commit
      database.commit();

      // Ensure that the people made it in correctly
      final ResultSet result1 = database.query("select from Person");
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
    } catch (IndexException e) {
      System.out.println("Exception: " + e);
      database.rollback();
    }

    final ResultSet result2 = database.command("select from Person");
    System.out.println("After transaction 2");
    while (result2.hasNext()) {
      System.out.println(result2.next());
    }
  }
}
