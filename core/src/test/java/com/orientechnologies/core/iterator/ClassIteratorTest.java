package com.orientechnologies.core.iterator;

import com.orientechnologies.DBTestBase;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.metadata.schema.clusterselection.ODefaultClusterSelectionStrategy;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.HashSet;
import java.util.Set;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 */
public class ClassIteratorTest extends DBTestBase {

  private Set<String> names;

  private void createPerson(final String iClassName, final String first) {
    // Create Person document
    db.begin();
    final YTEntityImpl personDoc = db.newInstance(iClassName);
    personDoc.field("First", first);
    personDoc.save();
    db.commit();
  }

  public void beforeTest() throws Exception {
    super.beforeTest();

    final YTSchema schema = db.getMetadata().getSchema();

    // Create Person class
    final YTClass personClass = schema.createClass("Person");
    personClass
        .createProperty(db, "First", YTType.STRING)
        .setMandatory(db, true)
        .setNotNull(db, true)
        .setMin(db, "1");

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
    final YTClass personClass = db.getMetadata().getSchema().getClass("Person");

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
    final ORecordIteratorClass<YTEntityImpl> personIter =
        new ORecordIteratorClassDescendentOrder<YTEntityImpl>(db, db, "Person", true);

    personIter.setRange(null, null); // open range

    int docNum = 0;
    // Explicit iterator loop.
    while (personIter.hasNext()) {
      final YTEntityImpl personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc);
    }

    Assert.assertTrue(names.isEmpty());
  }

  @Test
  public void testMultipleClusters() throws Exception {
    final YTClass personClass =
        db.getMetadata().getSchema().createClass("PersonMultipleClusters", 4, null);
    for (String name : names) {
      createPerson("PersonMultipleClusters", name);
    }

    final ORecordIteratorClass<YTEntityImpl> personIter =
        new ORecordIteratorClass<YTEntityImpl>(db, "PersonMultipleClusters", true);

    int docNum = 0;

    while (personIter.hasNext()) {
      final YTEntityImpl personDoc = personIter.next();
      Assert.assertTrue(names.contains(personDoc.field("First")));
      Assert.assertTrue(names.remove(personDoc.field("First")));
      System.out.printf("Doc %d: %s\n", docNum++, personDoc);
    }

    Assert.assertTrue(names.isEmpty());
  }
}
