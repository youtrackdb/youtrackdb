package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.IndexException;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

@Test
public class IndexConcurrentCommitTest extends BaseDBTest {

  @Parameters(value = "remote")
  public IndexConcurrentCommitTest(boolean remote) {
    super(remote);
  }

  public void testConcurrentUpdate() {
    SchemaClass personClass = db.getMetadata().getSchema().createClass("Person");
    personClass.createProperty(db, "ssn", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.UNIQUE);
    personClass.createProperty(db, "name", PropertyType.STRING)
        .createIndex(db, SchemaClass.INDEX_TYPE.NOTUNIQUE);

    try {
      // Transaction 1
      db.begin();

      // Insert two people in a transaction
      EntityImpl person1 = ((EntityImpl) db.newEntity("Person"));
      person1.field("name", "John Doe");
      person1.field("ssn", "111-11-1111");
      person1.save();

      EntityImpl person2 = ((EntityImpl) db.newEntity("Person"));
      person2.field("name", "Jane Doe");
      person2.field("ssn", "222-22-2222");
      person2.save();

      // Commit
      db.commit();

      // Ensure that the people made it in correctly
      final ResultSet result1 = db.query("select from Person");
      while (result1.hasNext()) {
        System.out.println(result1.next());
      }

      // Transaction 2
      db.begin();

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
      db.commit();

      System.out.println("Success!");
    } catch (IndexException e) {
      System.out.println("Exception: " + e);
      db.rollback();
    }

    final ResultSet result2 = db.command("select from Person");
    System.out.println("After transaction 2");
    while (result2.hasNext()) {
      System.out.println(result2.next());
    }
  }
}
