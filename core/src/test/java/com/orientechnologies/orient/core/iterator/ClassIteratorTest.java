package com.orientechnologies.orient.core.iterator;

import com.orientechnologies.BaseMemoryDatabase;
import com.orientechnologies.orient.core.metadata.schema.OClass;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.metadata.schema.OType;
import com.orientechnologies.orient.core.metadata.schema.clusterselection.ODefaultClusterSelectionStrategy;
import com.orientechnologies.orient.core.record.impl.ODocument;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ClassIteratorTest extends BaseMemoryDatabase {

  private Set<String> names;

  private void createPerson(final String iClassName, final String first) {
    // Create Person document
    db.begin();
    final ODocument personDoc = db.newInstance(iClassName);
    personDoc.field("First", first);
    personDoc.save();
    db.commit();
  }

  public void beforeTest() {
    super.beforeTest();

    final OSchema schema = db.getMetadata().getSchema();

    // Create Person class
    final OClass personClass = schema.createClass("Person");
    personClass
        .createProperty(db, "First", OType.STRING)
        .setMandatory(db, true)
        .setNotNull(db, true)
        .setMin("1");

    // Insert some data
    names = new HashSet<String>();
    names.add("Adam");
    names.add("Bob");
    names.add("Calvin");
    names.add("Daniel");

    for (String name : names) {
      createPerson("Person", name);
    }
  }

  @Test
  public void testDescendentOrderIteratorWithMultipleClusters() throws Exception {
    final OClass personClass = db.getMetadata().getSchema().getClass("Person");

    // empty old cluster but keep it attached
    personClass.truncate(db);

    // reload the data in a new 'test' cluster
    int testClusterId = db.addCluster("test");
    personClass.addClusterId(db, testClusterId);
    personClass.setClusterSelection(db, new ODefaultClusterSelectionStrategy());
    personClass.setDefaultClusterId(db, testClusterId);

    for (String name : names) {
      createPerson("Person", name);
    }

    // Use descending class iterator.
    final ORecordIteratorClass<ODocument> personIter =
        new ORecordIteratorClassDescendentOrder<ODocument>(db, db, "Person", true);

    personIter.setRange(null, null); // open range

    int docNum = 0;
    // Explicit iterator loop.
    while (personIter.hasNext()) {
      final ODocument personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc);
    }

    Assert.assertTrue(names.isEmpty());
  }

  @Test
  public void testMultipleClusters() throws Exception {
    final OClass personClass =
        db.getMetadata().getSchema().createClass("PersonMultipleClusters", 4, null);
    for (String name : names) {
      createPerson("PersonMultipleClusters", name);
    }

    final ORecordIteratorClass<ODocument> personIter =
        new ORecordIteratorClass<ODocument>(db, "PersonMultipleClusters", true);

    int docNum = 0;

    while (personIter.hasNext()) {
      final ODocument personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc);
    }

    Assert.assertTrue(names.isEmpty());
  }
}
