package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.index.OIndex;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTClass;
import com.jetbrains.youtrack.db.internal.core.metadata.schema.YTType;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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
public class LinkSetIndexTest extends DocumentDBBaseTest {

  @Parameters(value = "remote")
  public LinkSetIndexTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  public void setupSchema() {
    final YTClass ridBagIndexTestClass =
        database.getMetadata().getSchema().createClass("LinkSetIndexTestClass");

    ridBagIndexTestClass.createProperty(database, "linkSet", YTType.LINKSET);

    ridBagIndexTestClass.createIndex(database, "linkSetIndex", YTClass.INDEX_TYPE.NOTUNIQUE,
        "linkSet");
    database.close();
  }

  @BeforeMethod
  public void beforeMethod() {
    database = createSessionInstance();
  }

  @AfterMethod
  public void afterMethod() {
    checkEmbeddedDB();

    database.begin();
    database.command("DELETE FROM LinkSetIndexTestClass").close();
    database.commit();

    YTResultSet result = database.command("select from LinkSetIndexTestClass");
    Assert.assertEquals(result.stream().count(), 0);

    if (database.getStorage().isRemote()) {
      OIndex index =
          database.getMetadata().getIndexManagerInternal().getIndex(database, "linkSetIndex");
      Assert.assertEquals(index.getInternal().size(database), 0);
    }

    database.close();
  }

  public void testIndexLinkSet() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    try {
      database.begin();
      final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
      final Set<YTIdentifiable> linkSet = new HashSet<>();
      linkSet.add(database.bindToSession(docOne));
      linkSet.add(database.bindToSession(docTwo));

      document.field("linkSet", linkSet);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdate() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    final Set<YTIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(docOne);
    linkSetTwo.add(docThree);

    document.field("linkSet", linkSetTwo);
    document.save();
    database.commit();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();
    database.commit();

    try {
      database.begin();

      document = database.bindToSession(document);
      final Set<YTIdentifiable> linkSetTwo = new HashSet<>();
      linkSetTwo.add(database.bindToSession(docOne));
      linkSetTwo.add(database.bindToSession(docThree));

      document.field("linkSet", linkSetTwo);
      document.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docThree.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final Set<YTIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    document.field("linkSet", linkSetOne);
    document.save();
    database.commit();

    database.begin();

    document = database.bindToSession(document);
    final Set<YTIdentifiable> linkSetTwo = new HashSet<>();
    linkSetTwo.add(database.bindToSession(docOne));
    linkSetTwo.add(database.bindToSession(docThree));

    document.field("linkSet", linkSetTwo);
    document.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateAddItem() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);

    document.save();
    database.commit();

    database.begin();
    database
        .command(
            "UPDATE "
                + document.getIdentity()
                + " set linkSet = linkSet || "
                + docThree.getIdentity())
        .close();
    database.commit();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
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

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save();

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save();

    final EntityImpl docThree = new EntityImpl();
    docThree.save();

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    try {
      database.begin();
      EntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<YTIdentifiable>>field("linkSet").add(database.bindToSession(docThree));
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 3);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
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

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    database.begin();
    EntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<YTIdentifiable>>field("linkSet").add(database.bindToSession(docThree));
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    try {
      database.begin();
      EntityImpl loadedDocument = database.load(document.getIdentity());
      loadedDocument.<Set<YTIdentifiable>>field("linkSet").remove(docTwo);
      loadedDocument.save();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItemInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);
    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    database.begin();
    EntityImpl loadedDocument = database.load(document.getIdentity());
    loadedDocument.<Set<YTIdentifiable>>field("linkSet").remove(docTwo);
    loadedDocument.save();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetUpdateRemoveItem() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    database.begin();
    database
        .command("UPDATE " + document.getIdentity() + " remove linkSet = " + docTwo.getIdentity())
        .close();
    database.commit();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 1);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetRemove() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");

    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.commit();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexLinkSetRemoveInTx() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");

    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    try {
      database.begin();
      database.bindToSession(document).delete();
      database.commit();
    } catch (Exception e) {
      database.rollback();
      throw e;
    }

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 0);
  }

  public void testIndexLinkSetRemoveInTxRollback() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();

    linkSet.add(docOne);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    database.begin();
    database.bindToSession(document).delete();
    database.rollback();

    OIndex index = getIndex("linkSetIndex");
    Assert.assertEquals(index.getInternal().size(database), 2);

    Iterator<Object> keysIterator;
    try (Stream<Object> keyStream = index.getInternal().keyStream()) {
      keysIterator = keyStream.iterator();

      while (keysIterator.hasNext()) {
        YTIdentifiable key = (YTIdentifiable) keysIterator.next();
        if (!key.getIdentity().equals(docOne.getIdentity())
            && !key.getIdentity().equals(docTwo.getIdentity())) {
          Assert.fail("Unknown key found: " + key);
        }
      }
    }
  }

  public void testIndexLinkSetSQL() {
    checkEmbeddedDB();

    database.begin();
    final EntityImpl docOne = new EntityImpl();
    docOne.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docTwo = new EntityImpl();
    docTwo.save(database.getClusterNameById(database.getDefaultClusterId()));

    final EntityImpl docThree = new EntityImpl();
    docThree.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSetOne = new HashSet<>();
    linkSetOne.add(docOne);
    linkSetOne.add(docTwo);

    document.field("linkSet", linkSetOne);
    document.save();

    document = new EntityImpl("LinkSetIndexTestClass");
    final Set<YTIdentifiable> linkSet = new HashSet<>();
    linkSet.add(docThree);
    linkSet.add(docTwo);

    document.field("linkSet", linkSet);
    document.save();
    database.commit();

    YTResultSet result =
        database.query(
            "select * from LinkSetIndexTestClass where linkSet contains ?", docOne.getIdentity());

    List<YTIdentifiable> listResult =
        new ArrayList<>(result.next().<Set<YTIdentifiable>>getProperty("linkSet"));
    Assert.assertEquals(listResult.size(), 2);
    Assert.assertTrue(
        listResult.containsAll(Arrays.asList(docOne.getIdentity(), docTwo.getIdentity())));
  }
}
