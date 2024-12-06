package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.db.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.PropertyType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 * @since 21.03.12
 */
@Test
public class LinkListIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public LinkListIndexTest(@Optional Boolean remote) {
    // super(remote != null && remote);
    super(true);
  }

  @BeforeClass
  public void setupSchema() {
    final SchemaClass linkListIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkListIndexTestClass");

    linkListIndexTestClass.createProperty(database, "linkCollection", PropertyType.LINKLIST);

    linkListIndexTestClass.createIndex(database,
        "linkCollectionIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "linkCollection");
  }

  @AfterClass
  public void destroySchema() {
    database = acquireSession();
    database.getMetadata().getSchema().dropClass("LinkListIndexTestClass");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    database.begin();
    database.command("DELETE FROM LinkListIndexTestClass").close();
    database.commit();

    ResultSet result = database.query("select from LinkListIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!database.getStorage().isRemote()) {
      final Index index = getIndex("linkCollectionIndex");
      Assert.assertEquals(index.getInternal().size(database), 0);
    }

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    try {
      database.begin();
      final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity()) && !key.equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdate() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");

    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();
    database.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));

    document.save();
    database.commit();

    try {
      database.begin();
      document = database.bindToSession(document);
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));

    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();
    database.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItem() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkCollection = linkCollection || "
                + docThree.getIdentity())
        .close();
    database.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    try {
      database.begin();
      EntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<List<Identifiable>>field("linkCollection").add(docThree.getIdentity());
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    EntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<List<Identifiable>>field("linkCollection").add(docThree.getIdentity());
    loadedDocument.save();
    database.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    try {
      database.begin();
      EntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 1);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    EntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
    loadedDocument.save();
    database.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionUpdateRemoveItem() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    database.command(
        "UPDATE " + document.getIdentity() + " remove linkCollection = " + docTwo.getIdentity());
    database.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 1);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionRemove() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    document = database.bindToSession(document);
    document.delete();
    database.commit();

    Index index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    try {
      database.begin();
      document = database.bindToSession(document);
      document.delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.rollback();

    Index index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keyIterator;
    try (Stream<Object> indexKeyStream = index.getInternal().keyStream()) {
      keyIterator = indexKeyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexCollectionSQL() {
    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkListIndexTestClass");
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    database.commit();

    ResultSet result =
        database.query(
            "select * from LinkListIndexTestClass where linkCollection contains ?",
            docOne.getIdentity());
    Assert.assertEquals(
        Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()),
        result.next().<List>getProperty("linkCollection"));
  }
}
