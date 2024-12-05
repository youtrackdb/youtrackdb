/*
 *
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.orientechnologies.orient.test.database.auto;

import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.jetbrains.youtrack.db.internal.core.db.ODatabaseRecordThreadLocal;
import com.jetbrains.youtrack.db.internal.core.db.record.YTIdentifiable;
import com.jetbrains.youtrack.db.internal.core.db.record.ridbag.RidBag;
import com.jetbrains.youtrack.db.internal.core.engine.memory.OEngineMemory;
import com.jetbrains.youtrack.db.internal.core.storage.cache.local.OWOWCache;
import com.jetbrains.youtrack.db.internal.core.storage.disk.OLocalPaginatedStorage;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.testng.Assert;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.Optional;
import org.testng.annotations.Parameters;
import org.testng.annotations.Test;

/**
 *
 */
@Test
public class OSBTreeRidBagTest extends ORidBagTest {

  private int topThreshold;
  private int bottomThreshold;

  @Parameters(value = "remote")
  public OSBTreeRidBagTest(@Optional Boolean remote) {
    super(remote != null && remote);
  }

  @BeforeClass
  @Override
  public void beforeClass() throws Exception {
    ODatabaseRecordThreadLocal.instance().remove();
    super.beforeClass();
  }

  @BeforeMethod
  public void beforeMethod() throws IOException {
    topThreshold =
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, topThreshold);
      server.setGlobalConfiguration(
          GlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, bottomThreshold);
      server.close();
    }
  }

  public void testRidBagClusterDistribution() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) {
      return;
    }

    final int clusterIdOne = database.addCluster("clusterOne");

    EntityImpl docClusterOne = new EntityImpl();
    RidBag ridBagClusterOne = new RidBag(database);
    docClusterOne.field("ridBag", ridBagClusterOne);

    database.begin();
    docClusterOne.save("clusterOne");
    database.commit();

    final String directory = database.getStorage().getConfiguration().getDirectory();

    final OWOWCache wowCache =
        (OWOWCache) ((OLocalPaginatedStorage) (database.getStorage())).getWriteCache();

    final long fileId =
        wowCache.fileIdByName(
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterIdOne
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);
    final String fileName = wowCache.nativeFileNameById(fileId);
    assert fileName != null;
    final File ridBagOneFile = new File(directory, fileName);
    Assert.assertTrue(ridBagOneFile.exists());
  }

  public void testIteratorOverAfterRemove() {
    database.begin();
    EntityImpl scuti =
        new EntityImpl()
            .field("name", "UY Scuti");
    scuti.save(database.getClusterNameById(database.getDefaultClusterId()));
    EntityImpl cygni =
        new EntityImpl()
            .field("name", "NML Cygni");
    cygni.save(database.getClusterNameById(database.getDefaultClusterId()));
    EntityImpl scorpii =
        new EntityImpl()
            .field("name", "AH Scorpii");
    scorpii.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    scuti = database.bindToSession(scuti);
    cygni = database.bindToSession(cygni);
    scorpii = database.bindToSession(scorpii);

    HashSet<EntityImpl> expectedResult = new HashSet<EntityImpl>(Arrays.asList(scuti, scorpii));

    RidBag bag = new RidBag(database);
    bag.add(scuti);
    bag.add(cygni);
    bag.add(scorpii);

    EntityImpl doc = new EntityImpl();
    doc.field("ridBag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    doc = database.bindToSession(doc);
    bag = doc.field("ridBag");
    bag.remove(cygni);

    Set<EntityImpl> result = new HashSet<EntityImpl>();
    for (YTIdentifiable identifiable : bag) {
      result.add(identifiable.getRecord());
    }

    Assert.assertEquals(result, expectedResult);
  }

  public void testRidBagConversion() {
    final int oldThreshold =
        GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);

    database.begin();
    EntityImpl doc_1 = new EntityImpl();
    doc_1.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl doc_2 = new EntityImpl();
    doc_2.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl doc_3 = new EntityImpl();
    doc_3.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl doc_4 = new EntityImpl();
    doc_4.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl doc = new EntityImpl();

    RidBag bag = new RidBag(database);
    bag.add(doc_1);
    bag.add(doc_2);
    bag.add(doc_3);
    bag.add(doc_4);

    doc.field("ridBag", bag);
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    EntityImpl doc_5 = new EntityImpl();
    doc_5.save(database.getClusterNameById(database.getDefaultClusterId()));

    EntityImpl doc_6 = new EntityImpl();
    doc_6.save(database.getClusterNameById(database.getDefaultClusterId()));

    bag = doc.field("ridBag");
    bag.add(doc_5);
    bag.add(doc_6);

    doc.save();
    database.commit();

    database.begin();
    doc = database.bindToSession(doc);
    bag = doc.field("ridBag");
    Assert.assertEquals(bag.size(), 6);

    List<YTIdentifiable> docs = new ArrayList<YTIdentifiable>();

    docs.add(doc_1.getIdentity());
    docs.add(doc_2.getIdentity());
    docs.add(doc_3.getIdentity());
    docs.add(doc_4.getIdentity());
    docs.add(doc_5.getIdentity());
    docs.add(doc_6.getIdentity());

    for (YTIdentifiable rid : bag) {
      Assert.assertTrue(docs.remove(rid));
    }

    Assert.assertTrue(docs.isEmpty());

    GlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(oldThreshold);
    database.rollback();
  }

  public void testRidBagDelete() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) {
      return;
    }

    float reuseTrigger =
        GlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.getValueAsFloat();
    GlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(Float.MIN_VALUE);

    EntityImpl realDoc = new EntityImpl();
    RidBag realDocRidBag = new RidBag(database);
    realDoc.field("ridBag", realDocRidBag);

    for (int i = 0; i < 10; i++) {
      EntityImpl docToAdd = new EntityImpl();
      realDocRidBag.add(docToAdd);
    }

    assertEmbedded(realDocRidBag.isEmbedded());

    database.begin();
    realDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final int clusterId = database.addCluster("ridBagDeleteTest");

    EntityImpl testDocument = crateTestDeleteDoc(realDoc);
    database.freeze();
    database.release();

    final String directory = database.getStorage().getConfiguration().getDirectory();

    File testRidBagFile =
        new File(
            directory,
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterId
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);
    long testRidBagSize = testRidBagFile.length();

    for (int i = 0; i < 100; i++) {
      database.begin();
      database.bindToSession(testDocument).delete();
      database.commit();

      testDocument = crateTestDeleteDoc(realDoc);
    }

    database.freeze();
    database.release();

    GlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(reuseTrigger);
    testRidBagFile =
        new File(
            directory,
            OSBTreeCollectionManagerShared.FILE_NAME_PREFIX
                + clusterId
                + OSBTreeCollectionManagerShared.FILE_EXTENSION);

    Assert.assertEquals(testRidBagFile.length(), testRidBagSize);

    realDoc = database.load(realDoc.getIdentity());
    RidBag ridBag = realDoc.field("ridBag");
    Assert.assertEquals(ridBag.size(), 10);
  }

  private EntityImpl crateTestDeleteDoc(EntityImpl realDoc) {
    EntityImpl testDocument = new EntityImpl();
    RidBag highLevelRidBag = new RidBag(database);
    testDocument.field("ridBag", highLevelRidBag);
    realDoc = database.bindToSession(realDoc);
    testDocument.field("realDoc", realDoc);

    database.begin();
    testDocument.save("ridBagDeleteTest");
    database.commit();

    return testDocument;
  }

  @Override
  protected void assertEmbedded(boolean isEmbedded) {
    Assert.assertTrue((!isEmbedded || ODatabaseRecordThreadLocal.instance().get().isRemote()));
  }
}
