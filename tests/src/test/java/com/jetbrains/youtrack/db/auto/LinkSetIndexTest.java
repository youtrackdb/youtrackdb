package com.jetbrains.youtrack.db.auto;

import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;

/**
 * @since 3/28/14
 */
@SuppressWarnings("deprecation")
public class LinkSetIndexTest extends BaseDBTest {

  @Parameters(value = "remote")
  public LinkSetIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final var ridBagIndexTestClass =
        db.getMetadata().getSchema().createClass("LinkSetIndexTestClass");

    ridBagIndexTestClass.createProperty(db, "linkSet", PropertyType.LINKSET);

    ridBagIndexTestClass.createIndex(db, "linkSetIndex", SchemaClass.INDEX_TYPE.NOTUNIQUE,
        "linkSet");
    db.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    db = createSessionInstance();
  }

  @AfterMethod
  public void afterMethod() {
    checkEmbeddedDB();

    db.begin();
    db.command("DELETE FROM LinkSetIndexTestClass").close();
    db.commit();

    var result = db.command("select from LinkSetIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (db.getStorage().isRemote()) {
      var index =
          db.getMetadata().getIndexManagerInternal().getIndex(db, "linkSetIndex");
      Assert.assertEquals(index.getInternal().size(db), 0);
    }

    db.close();
  }

  public void testIndexLinkSet() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();
    db.commit();

    try {
      db.begin();
      final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
      final Set<Identifiable> linkSet = new HashSet<>();
      linkSet.add(db.bindToSession(docOne));
      linkSet.add(db.bindToSession(docTwo));

      document.field("linkSet", linkSet);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdate() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    final Set<Identifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();
    db.commit();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();
    db.commit();

    try {
      db.begin();

      document = db.bindToSession(document);
      final Set<Identifiable> linkSetTwo = new HashSet<>();
      linkSetTwo.add(db.bindToSession(docOne));
      linkSetTwo.add(db.bindToSession(docThree));

      document.field("linkSet", linkSetTwo);
      document.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final Set<Identifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    document.field("linkSet", linkSetOne);
    document.save();
    db.commit();

    db.begin();

    document = db.bindToSession(document);
    final Set<Identifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(db.bindToSession(docOne));
    linkSetTwo.add(db.bindToSession(docThree));

    document.field("linkSet", linkSetTwo);
    document.save();
    db.rollback();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItem() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);

    document.save();
    db.commit();

    db.begin();
    db
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkSet = linkSet || "
                + docThree.getIdentity())
        .close();
    db.commit();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<Set<Identifiable>>field("linkSet").add(db.bindToSession(docThree));
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 3);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<Set<Identifiable>>field("linkSet").add(db.bindToSession(docThree));
    loadedDocument.save();
    db.rollback();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    try {
      db.begin();
      EntityImpl loadedDocument = db.load(document.getIdentity());
      loadedDocument.<Set<Identifiable>>field("linkSet").remove(docTwo);
      loadedDocument.save();
      db.commit();
    } catch (Exception e) {
      db.rollback();
      throw e;
    }

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    db.begin();
    EntityImpl loadedDocument = db.load(document.getIdentity());
    loadedDocument.<Set<Identifiable>>field("linkSet").remove(docTwo);
    loadedDocument.save();
    db.rollback();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItem() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    db.begin();
    db
        .command("UPDATE " + document.getIdentity() + " remove linkSet = " + docTwo.getIdentity())
        .close();
    db.commit();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 1);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetRemove() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));

    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.commit();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexLinkSetRemoveInTx() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));

    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
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

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 0);
  }

  public void testIndexLinkSetRemoveInTxRollback() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();

    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    db.begin();
    db.bindToSession(document).delete();
    db.rollback();

    var index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(db), 2);

    Iterator<Object> keysIterator;
    try (var keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        var key = (Identifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetSQL() {
    checkEmbeddedDB();

    db.begin();
    final var docOne = ((EntityImpl) db.newEntity());
    docOne.save();

    final var docTwo = ((EntityImpl) db.newEntity());
    docTwo.save();

    final var docThree = ((EntityImpl) db.newEntity());
    docThree.save();

    var document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    document = ((EntityImpl) db.newEntity("LinkSetIndexTestClass"));
    final Set<Identifiable> linkSet = new HashSet<>();
    linkSet.add(docThree);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    db.commit();

    var result =
        db.query(
            "select * from LinkSetIndexTestClass where linkSet contains ?", docOne.getIdentity());

    List<Identifiable> listResult =
        new ArrayList<>(result.next().<Set<Identifiable>>getProperty("linkSet"));
    Assert.assertEquals(listResult.size(), 2);
    Assert.assertTrue(
        listResult.containsAll(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
  }
}
