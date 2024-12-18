package com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.api.exception.ConcurrentModificationException;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class RidBagAtomicUpdateTest extends DbTestBase {

  private int topThreshold;
  private int bottomThreshold;

  @Before
  public void beforeMethod() {
    topThreshold =
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @After
  public void afterMethod() {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);
  }

  @Test
  public void testAddTwoNewDocuments() {
    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoNewDocumentsWithCME() throws Exception {
    db.begin();

    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    cmeDoc.field("v", "v234");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    generateCME(cmeDoc.getIdentity());

    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 0);
  }

  @Test
  public void testAddTwoAdditionalNewDocuments() {
    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    EntityImpl docThree = (EntityImpl) db.newEntity();
    EntityImpl docFour = (EntityImpl) db.newEntity();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<Identifiable> iterator = ridBag.iterator();
    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddingDocsDontUpdateVersion() {
    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    EntityImpl docTwo = (EntityImpl) db.newEntity();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    db.begin();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
    db.rollback();
  }

  @Test
  public void testAddingDocsDontUpdateVersionInTx() {
    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();

    ridBag.add(docOne);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    final int version = rootDoc.getVersion();

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    EntityImpl docTwo = (EntityImpl) db.newEntity();
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);

    db.begin();

    Assert.assertEquals(ridBag.size(), 2);
    Assert.assertEquals(rootDoc.getVersion(), version);
    db.rollback();
  }

  @Test
  public void testAddTwoAdditionalNewDocumentsWithCME() throws Exception {
    db.begin();
    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    EntityImpl docThree = (EntityImpl) db.newEntity();
    EntityImpl docFour = (EntityImpl) db.newEntity();

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    Iterator<Identifiable> iterator = ridBag.iterator();
    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoSavedDocuments() {
    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));
    EntityImpl docTwo = (EntityImpl) db.newEntity();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
  }

  @Test
  public void testAddTwoAdditionalSavedDocuments() {
    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();
    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    EntityImpl docThree = (EntityImpl) db.newEntity();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    EntityImpl docFour = (EntityImpl) db.newEntity();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    Iterator<Identifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddTwoAdditionalSavedDocumentsWithCME() throws Exception {
    db.begin();
    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    EntityImpl docTwo = (EntityImpl) db.newEntity();

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    db.begin();

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docThree = (EntityImpl) db.newEntity();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));
    EntityImpl docFour = (EntityImpl) db.newEntity();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    generateCME(cmeDoc.getIdentity());

    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Assert.assertEquals(ridBag.size(), 2);

    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    Iterator<Identifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocuments() {
    db.begin();

    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docTwo = (EntityImpl) db.newEntity();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    EntityImpl docThree = (EntityImpl) db.newEntity();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFour = (EntityImpl) db.newEntity();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docThreeOne = (EntityImpl) db.newEntity();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docThreeTwo = (EntityImpl) db.newEntity();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    RidBag ridBagThree = new RidBag(db);
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFourOne = (EntityImpl) db.newEntity();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFourTwo = (EntityImpl) db.newEntity();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    RidBag ridBagFour = new RidBag(db);
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.rollback();

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<Identifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testAddInternalDocumentsAndSubDocumentsWithCME() throws Exception {
    db.begin();
    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    EntityImpl docOne = (EntityImpl) db.newEntity();
    docOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docTwo = (EntityImpl) db.newEntity();
    docTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    ridBag.add(docOne);
    ridBag.add(docTwo);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    long recordsCount = db.countClusterElements(db.getDefaultClusterId());

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v2");
    cmeDoc.save();

    EntityImpl docThree = (EntityImpl) db.newEntity();
    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFour = (EntityImpl) db.newEntity();
    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    rootDoc = db.bindToSession(rootDoc);
    ridBag = rootDoc.field("ridBag");

    ridBag.add(docThree);
    ridBag.add(docFour);

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docThreeOne = (EntityImpl) db.newEntity();
    docThreeOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docThreeTwo = (EntityImpl) db.newEntity();
    docThreeTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    RidBag ridBagThree = new RidBag(db);
    ridBagThree.add(docThreeOne);
    ridBagThree.add(docThreeTwo);
    docThree.field("ridBag", ridBagThree);

    docThree.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFourOne = (EntityImpl) db.newEntity();
    docFourOne.save(db.getClusterNameById(db.getDefaultClusterId()));

    EntityImpl docFourTwo = (EntityImpl) db.newEntity();
    docFourTwo.save(db.getClusterNameById(db.getDefaultClusterId()));

    RidBag ridBagFour = new RidBag(db);
    ridBagFour.add(docFourOne);
    ridBagFour.add(docFourTwo);

    docFour.field("ridBag", ridBagFour);

    docFour.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    Assert.assertEquals(db.countClusterElements(db.getDefaultClusterId()), recordsCount);
    List<Identifiable> addedDocs = new ArrayList<Identifiable>(Arrays.asList(docOne, docTwo));

    rootDoc = db.load(rootDoc.getIdentity());
    ridBag = rootDoc.field("ridBag");

    Iterator<Identifiable> iterator = ridBag.iterator();
    Assert.assertTrue(addedDocs.remove(iterator.next()));
    Assert.assertTrue(addedDocs.remove(iterator.next()));
  }

  @Test
  public void testRandomChangedInTxLevel2() {
    testRandomChangedInTx(2);
  }

  @Test
  public void testRandomChangedInTxLevel1() {
    testRandomChangedInTx(1);
  }

  private void testRandomChangedInTx(final int levels) {
    Random rnd = new Random();

    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<Identifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<Identifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<LevelKey, List<Identifiable>>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);
    db.rollback();

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testRandomChangedInTxWithCME() throws Exception {
    db.begin();
    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Random rnd = new Random();

    final int levels = rnd.nextInt(2) + 1;
    final List<Integer> amountOfAddedDocsPerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfAddedDocsAfterSavePerLevel = new ArrayList<Integer>();
    final List<Integer> amountOfDeletedDocsPerLevel = new ArrayList<Integer>();
    Map<LevelKey, List<Identifiable>> addedDocPerLevel =
        new HashMap<LevelKey, List<Identifiable>>();

    for (int i = 0; i < levels; i++) {
      amountOfAddedDocsPerLevel.add(rnd.nextInt(5) + 10);
      amountOfAddedDocsAfterSavePerLevel.add(rnd.nextInt(5) + 5);
      amountOfDeletedDocsPerLevel.add(rnd.nextInt(5) + 5);
    }

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "v");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    EntityImpl rootDoc = (EntityImpl) db.newEntity();
    createDocsForLevel(amountOfAddedDocsPerLevel, 0, levels, addedDocPerLevel, rootDoc);
    db.commit();

    addedDocPerLevel = new HashMap<>(addedDocPerLevel);

    rootDoc = db.load(rootDoc.getIdentity());
    db.begin();
    deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, 0, levels, rootDoc, rnd);
    addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, 0, levels, rootDoc);

    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "vn");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    rootDoc = db.load(rootDoc.getIdentity());
    assertDocsAfterRollback(0, levels, addedDocPerLevel, rootDoc);
  }

  @Test
  public void testFromEmbeddedToSBTreeRollback() {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    db.begin();
    EntityImpl document = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeTXWithCME() throws Exception {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    db.begin();
    EntityImpl cmeDocument = (EntityImpl) db.newEntity();
    cmeDocument.field("v", "v1");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    EntityImpl document = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    EntityImpl staleDocument = db.load(cmeDocument.getIdentity());
    Assert.assertNotSame(staleDocument, cmeDocument);

    db.begin();

    cmeDocument = db.bindToSession(cmeDocument);
    cmeDocument.field("v", "v234");
    cmeDocument.save(db.getClusterNameById(db.getDefaultClusterId()));

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    generateCME(cmeDocument.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromEmbeddedToSBTreeWithCME() throws Exception {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(5);

    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    db.begin();
    EntityImpl document = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    Assert.assertEquals(docsToAdd.size(), 3);
    Assert.assertTrue(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    var rid = document.getIdentity();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 3; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
    }

    Assert.assertTrue(document.isDirty());
    try {
      generateCME(rid);
      document.save(db.getClusterNameById(db.getDefaultClusterId()));
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertTrue(ridBag.isEmbedded());
    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void generateCME(RID rid) throws InterruptedException {
    var th =
        new Thread(
            () -> {
              try (var session = db.copy()) {
                session.activateOnCurrentThread();
                session.begin();
                EntityImpl cmeDocument = session.load(rid);

                cmeDocument.field("v", "v1");
                cmeDocument.save(session.getClusterNameById(session.getDefaultClusterId()));
                session.commit();
              }
            });
    th.start();
    th.join();
  }

  @Test
  public void testFromSBTreeToEmbeddedRollback() {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    db.begin();
    EntityImpl document = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertFalse(ridBag.isEmbedded());

    document = db.load(document.getIdentity());

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 4; i++) {
      Identifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.rollback();

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  @Test
  public void testFromSBTreeToEmbeddedTxWithCME() throws Exception {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(7);

    db.begin();
    EntityImpl cmeDoc = (EntityImpl) db.newEntity();
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    List<Identifiable> docsToAdd = new ArrayList<Identifiable>();

    EntityImpl document = (EntityImpl) db.newEntity();

    RidBag ridBag = new RidBag(db);
    document.field("ridBag", ridBag);
    document.save(db.getClusterNameById(db.getDefaultClusterId()));
    db.commit();

    db.begin();
    document = db.bindToSession(document);
    ridBag = document.field("ridBag");

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));
      ridBag.add(docToAdd);
      docsToAdd.add(docToAdd);
    }

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    db.commit();

    Assert.assertEquals(docsToAdd.size(), 10);
    Assert.assertFalse(ridBag.isEmbedded());

    document = db.load(document.getIdentity());
    document.field("ridBag");

    db.begin();
    cmeDoc = db.bindToSession(cmeDoc);
    cmeDoc.field("v", "sd");
    cmeDoc.save(db.getClusterNameById(db.getDefaultClusterId()));

    document = db.bindToSession(document);
    ridBag = document.field("ridBag");
    for (int i = 0; i < 4; i++) {
      Identifiable docToRemove = docsToAdd.get(i);
      ridBag.remove(docToRemove);
    }

    Assert.assertTrue(document.isDirty());

    document.save(db.getClusterNameById(db.getDefaultClusterId()));

    generateCME(cmeDoc.getIdentity());
    try {
      db.commit();
      Assert.fail();
    } catch (ConcurrentModificationException e) {
    }

    document = db.load(document.getIdentity());
    ridBag = document.field("ridBag");

    Assert.assertFalse(ridBag.isEmbedded());

    for (Identifiable identifiable : ridBag) {
      Assert.assertTrue(docsToAdd.remove(identifiable));
    }

    Assert.assertTrue(docsToAdd.isEmpty());
  }

  private void createDocsForLevel(
      final List<Integer> amountOfAddedDocsPerLevel,
      int level,
      int levels,
      Map<LevelKey, List<Identifiable>> addedDocPerLevel,
      EntityImpl rootDoc) {

    int docs = amountOfAddedDocsPerLevel.get(level);

    List<Identifiable> addedDocs = new ArrayList<Identifiable>();
    addedDocPerLevel.put(new LevelKey(rootDoc.getIdentity(), level), addedDocs);

    RidBag ridBag = new RidBag(db);
    rootDoc.field("ridBag", ridBag);

    for (int i = 0; i < docs; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      addedDocs.add(docToAdd.getIdentity());
      ridBag.add(docToAdd);

      if (level + 1 < levels) {
        createDocsForLevel(
            amountOfAddedDocsPerLevel, level + 1, levels, addedDocPerLevel, docToAdd);
      }
    }

    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void deleteDocsForLevel(
      DatabaseSessionInternal db,
      List<Integer> amountOfDeletedDocsPerLevel,
      int level,
      int levels,
      EntityImpl rootDoc,
      Random rnd) {
    rootDoc = db.bindToSession(rootDoc);
    RidBag ridBag = rootDoc.field("ridBag");
    Iterator<Identifiable> iter = ridBag.iterator();
    while (iter.hasNext()) {
      Identifiable identifiable = iter.next();
      EntityImpl doc = identifiable.getRecord(db);
      if (level + 1 < levels) {
        deleteDocsForLevel(db, amountOfDeletedDocsPerLevel, level + 1, levels, doc, rnd);
      }
    }

    int docs = amountOfDeletedDocsPerLevel.get(level);

    int k = 0;
    Iterator<Identifiable> iterator = ridBag.iterator();
    while (k < docs && iterator.hasNext()) {
      iterator.next();

      if (rnd.nextBoolean()) {
        iterator.remove();
        k++;
      }

      if (!iterator.hasNext()) {
        iterator = ridBag.iterator();
      }
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void addDocsForLevel(
      DatabaseSessionInternal db,
      List<Integer> amountOfAddedDocsAfterSavePerLevel,
      int level,
      int levels,
      EntityImpl rootDoc) {
    rootDoc = db.bindToSession(rootDoc);
    RidBag ridBag = rootDoc.field("ridBag");

    for (Identifiable identifiable : ridBag) {
      EntityImpl doc = identifiable.getRecord(db);
      if (level + 1 < levels) {
        addDocsForLevel(db, amountOfAddedDocsAfterSavePerLevel, level + 1, levels, doc);
      }
    }

    int docs = amountOfAddedDocsAfterSavePerLevel.get(level);
    for (int i = 0; i < docs; i++) {
      EntityImpl docToAdd = (EntityImpl) db.newEntity();
      docToAdd.save(db.getClusterNameById(db.getDefaultClusterId()));

      ridBag.add(docToAdd);
    }
    rootDoc.save(db.getClusterNameById(db.getDefaultClusterId()));
  }

  private void assertDocsAfterRollback(
      int level,
      int levels,
      Map<LevelKey, List<Identifiable>> addedDocPerLevel,
      EntityImpl rootDoc) {
    RidBag ridBag = rootDoc.field("ridBag");
    List<Identifiable> addedDocs =
        new ArrayList<Identifiable>(
            addedDocPerLevel.get(new LevelKey(rootDoc.getIdentity(), level)));

    for (Identifiable identifiable : ridBag) {
      EntityImpl doc = identifiable.getRecord(db);
      if (level + 1 < levels) {
        assertDocsAfterRollback(level + 1, levels, addedDocPerLevel, doc);
      } else {
        Assert.assertNull(doc.field("ridBag"));
      }

      Assert.assertTrue(addedDocs.remove(doc));
    }

    Assert.assertTrue(addedDocs.isEmpty());
  }

  private final class LevelKey {

    private final RID rid;
    private final int level;

    private LevelKey(RID rid, int level) {
      this.rid = rid;
      this.level = level;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      LevelKey levelKey = (LevelKey) o;

      if (level != levelKey.level) {
        return false;
      }
      return rid.equals(levelKey.rid);
    }

    @Override
    public int hashCode() {
      int result = rid.hashCode();
      result = 31 * result + level;
      return result;
    }
  }
}
