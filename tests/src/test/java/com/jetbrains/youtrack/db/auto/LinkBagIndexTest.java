package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
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
 * @since 1/30/14
 */
@Test
public class LinkBagIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public LinkBagIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final SchemaClass ridBagIndexTestClass =
        db.getMetadata().getSchema().createClass("RidBagIndexTestClass");

    ridBagIndexTestClass.createProperty(db, "ridBag", PropertyType.LINKBAG);

    ridBagIndexTestClass.createIndex(db, "ridBagIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "ridBag");

    db.close();
  }

  @AfterClass
  public void destroySchema() {
    if (db.isClosed()) {
      db = acquireSession();
    }

    db.getMetadata().getSchema().dropClass("RidBagIndexTestClass");
    db.close();
  }

  @AfterMethod
  public void afterMethod() {
    db.begin();
    db.command("DELETE FROM RidBagIndexTestClass").close();
    db.commit();

    ResultSet result = db.query("select from RidBagIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!db.getStorage().isRemote()) {
      final Index index = getIndex("ridBagIndex");
      Assert.assertEquals(index.getInternal().size(db), 0);
    }
  }

  public void testIndexRidBag() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();
    try {
      final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
      final RidBag ridBag = new RidBag(db);
      ridBag.add(docOne);
      ridBag.add(docTwo);

      document.field("ridBag", ridBag);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdate() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBagOne = new RidBag(db);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);

    document.save();
    db.commit();

    db.begin();
    final RidBag ridBagTwo = new RidBag(db);
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document = db.bindToSession(document);
    document.field("ridBag", ridBagTwo);

    db.bindToSession(document).save();
    db.commit();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBagOne = new RidBag(db);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);

    document.save();
    db.commit();

    try {
      db.begin();

      document = db.bindToSession(document);
      final RidBag ridBagTwo = new RidBag(db);
      ridBagTwo.add(docOne);
      ridBagTwo.add(docThree);

      document.field("ridBag", ridBagTwo);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final RidBag ridBagOne = new RidBag(db);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    document.field("ridBag", ridBagOne);
    document.save();
    Assert.assertTrue(db.commit());

    db.begin();
    document = db.bindToSession(document);
    final RidBag ridBagTwo = new RidBag(db);
    ridBagTwo.add(docOne);
    ridBagTwo.add(docThree);

    document.field("ridBag", ridBagTwo);
    document.save();
    db.rollback();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateAddItem() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set ridBag = ridBag || "
                + docThree.getIdentity())
        .close();
    db.commit();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

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

  public void testIndexRidBagUpdateAddItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    try {
      db.begin();
      docThree = db.bindToSession(docThree);
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<RidBag>field("ridBag").add(docThree);
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

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

  public void testIndexRidBagUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    db.begin();
    docThree = db.bindToSession(docThree);
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<RidBag>field("ridBag").add(docThree);
    loadedDocument.save();
    db.rollback();

    final Index index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(db), 2);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<RidBag>field("ridBag").remove(docTwo);
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(db), 1);
    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);
    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<RidBag>field("ridBag").remove(docTwo);
    loadedDocument.save();
    db.rollback();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItem() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    //noinspection deprecation
    db.begin();
    db
        .command("UPDATE " + document.getIdentity() + " remove ridBag = " + docTwo.getIdentity())
        .close();
    db.commit();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagRemove() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));

    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    document.delete();
    db.commit();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexRidBagRemoveInTx() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));

    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    try {
      db.begin();
      db.bindToSession(document).delete();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexRidBagRemoveInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBag = new RidBag(db);
    ridBag.add(docOne);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.rollback();

    final Index index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        Identifiable key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagSQL() {
    db.begin();
    final EntityImpl docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final EntityImpl docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final EntityImpl docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    EntityImpl document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final RidBag ridBagOne = new RidBag(db);
    ridBagOne.add(docOne);
    ridBagOne.add(docTwo);

    document.field("ridBag", ridBagOne);
    document.save();

    document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    RidBag ridBag = new RidBag(db);
    ridBag.add(docThree);
    ridBag.add(docTwo);

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    ResultSet result =
        db.query(
            "select * from RidBagIndexTestClass where ridBag contains ?", docOne.getIdentity());
    Result res = result.next();

    List<Identifiable> listResult = new ArrayList<>();
    for (Identifiable identifiable : res.<RidBag>getProperty("ridBag")) {
      listResult.add(identifiable);
    }
    result.close();

    Assert.assertEquals(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()), listResult);
  }
}
