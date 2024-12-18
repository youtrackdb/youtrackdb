package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
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
public class LinkListIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public LinkListIndexTest(@Optional Boolean remote) {
    // super(remote != null && remote);
    super(true);
  }

  @BeforeClass
  public void setupSchema() {
    final SchemaClass linkListIndexTestClass =
        db.getMetadata().getSchema().createClass("LinkListIndexTestClass");

    linkListIndexTestClass.createProperty(db, "linkCollection", PropertyType.LINKLIST);

    linkListIndexTestClass.createIndex(db,
        "linkCollectionIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE, "linkCollection");
  }

  @AfterClass
  public void destroySchema() {
    db = acquireSession();
    db.getMetadata().getSchema().dropClass("LinkListIndexTestClass");
  }

  @AfterMethod
  public void afterMethod() throws Exception {
    db.begin();
    db.command("DELETE FROM LinkListIndexTestClass").close();
    db.commit();

    ResultSet result = db.query("select from LinkListIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!db.getStorage().isRemote()) {
      final Index index = getIndex("linkCollectionIndex");
      Assert.assertEquals(index.getInternal().size(db), 0);
    }

    super.afterMethod();
  }

  public void testIndexCollection() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    try {
      db.begin();
      final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));

    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();

    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();
    db.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));

    document.save();
    db.commit();

    try {
      db.begin();
      document = db.bindToSession(document);
      document.field(
          "linkCollection",
          new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));

    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docThree.getIdentity())));
    document.save();
    db.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkCollection = linkCollection || "
                + docThree.getIdentity())
        .close();
    db.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<List<Identifiable>>field("linkCollection").add(docThree.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<List<Identifiable>>field("linkCollection").add(docThree.getIdentity());
    loadedDocument.save();
    db.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<List>field("linkCollection").remove(docTwo.getIdentity());
    loadedDocument.save();
    db.rollback();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    db.command(
        "UPDATE " + document.getIdentity() + " remove linkCollection = " + docTwo.getIdentity());
    db.commit();

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

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

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    document.delete();
    db.commit();

    Index index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexCollectionRemoveInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    try {
      db.begin();
      document = db.bindToSession(document);
      document.delete();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    Index index = getIndex("linkCollectionIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexCollectionRemoveInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.rollback();

    Index index = getIndex("linkCollectionIndex");

    Assert.assertEquals(index.getInternal().size(db), 2);

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
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    final EntityImpl document = ((EntityImpl) db.newEntity("LinkListIndexTestClass"));
    document.field(
        "linkCollection",
        new ArrayList<>(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
    document.save();
    db.commit();

    ResultSet result =
        db.query(
            "select * from LinkListIndexTestClass where linkCollection contains ?",
            docOne.getIdentity());
    Assert.assertEquals(
        Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()),
        result.next().<List>getProperty("linkCollection"));
  }
}
