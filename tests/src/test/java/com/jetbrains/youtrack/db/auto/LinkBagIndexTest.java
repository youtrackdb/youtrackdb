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
    final var ridBagIndexTestClass =
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

    var result = db.query("select from RidBagIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (!db.getStorage().isRemote()) {
      final var index = getIndex("ridBagIndex");
      Assert.assertEquals(index.getInternal().size(db), 0);
    }
  }

  public void testIndexRidBag() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();
    try {
      final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
      final var ridBag = new RidBag(db);
      ridBag.add(docOne.getIdentity());
      ridBag.add(docTwo.getIdentity());

      document.field("ridBag", ridBag);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBagOne = new RidBag(db);
    ridBagOne.add(docOne.getIdentity());
    ridBagOne.add(docTwo.getIdentity());

    document.field("ridBag", ridBagOne);

    document.save();
    db.commit();

    db.begin();
    final var ridBagTwo = new RidBag(db);
    ridBagTwo.add(docOne.getIdentity());
    ridBagTwo.add(docThree.getIdentity());

    document = db.bindToSession(document);
    document.field("ridBag", ridBagTwo);

    db.bindToSession(document).save();
    db.commit();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBagOne = new RidBag(db);
    ridBagOne.add(docOne.getIdentity());
    ridBagOne.add(docTwo.getIdentity());

    document.field("ridBag", ridBagOne);

    document.save();
    db.commit();

    try {
      db.begin();

      document = db.bindToSession(document);
      final var ridBagTwo = new RidBag(db);
      ridBagTwo.add(docOne.getIdentity());
      ridBagTwo.add(docThree.getIdentity());

      document.field("ridBag", ridBagTwo);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var ridBagOne = new RidBag(db);
    ridBagOne.add(docOne.getIdentity());
    ridBagOne.add(docTwo.getIdentity());

    var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    document.field("ridBag", ridBagOne);
    document.save();
    Assert.assertTrue(db.commit());

    db.begin();
    document = db.bindToSession(document);
    final var ridBagTwo = new RidBag(db);
    ridBagTwo.add(docOne.getIdentity());
    ridBagTwo.add(docThree.getIdentity());

    document.field("ridBag", ridBagTwo);
    document.save();
    db.rollback();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());
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

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    try {
      db.begin();
      docThree = db.bindToSession(docThree);
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<RidBag>field("ridBag").add(docThree.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    db.begin();
    docThree = db.bindToSession(docThree);
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<RidBag>field("ridBag").add(docThree.getIdentity());
    loadedDocument.save();
    db.rollback();

    final var index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(db), 2);
    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());
    document.field("ridBag", ridBag);

    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<RidBag>field("ridBag").remove(docTwo.getIdentity());
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    final var index = getIndex("ridBagIndex");

    Assert.assertEquals(index.getInternal().size(db), 1);
    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());
    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<RidBag>field("ridBag").remove(docTwo.getIdentity());
    loadedDocument.save();
    db.rollback();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
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
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    //noinspection deprecation
    db.begin();
    db
        .command("UPDATE " + document.getIdentity() + " remove ridBag = " + docTwo.getIdentity())
        .close();
    db.commit();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagRemove() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));

    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);
    document.save();
    document.delete();
    db.commit();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexRidBagRemoveInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));

    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

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

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexRidBagRemoveInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBag = new RidBag(db);
    ridBag.add(docOne.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.rollback();

    final var index = getIndex("ridBagIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    final Iterator<Object> keyIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keyIterator = keyStream.iterator();

      while (keyIterator.hasNext()) {
        var key = (Identifiable) keyIterator.next();

        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexRidBagSQL() {
    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    var document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    final var ridBagOne = new RidBag(db);
    ridBagOne.add(docOne.getIdentity());
    ridBagOne.add(docTwo.getIdentity());

    document.field("ridBag", ridBagOne);
    document.save();

    document = ((EntityImpl) db.newEntity("RidBagIndexTestClass"));
    var ridBag = new RidBag(db);
    ridBag.add(docThree.getIdentity());
    ridBag.add(docTwo.getIdentity());

    document.field("ridBag", ridBag);
    document.save();
    db.commit();

    var result =
        db.query(
            "select * from RidBagIndexTestClass where ridBag contains ?", docOne.getIdentity());
    var res = result.next();

    List<Identifiable> listResult = new ArrayList<>();
    for (Identifiable identifiable : res.<RidBag>getProperty("ridBag")) {
      listResult.add(identifiable);
    }
    result.close();

    Assert.assertEquals(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity()), listResult);
  }
}
