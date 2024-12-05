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

import com.orientechnologies.orient.client.remote.OEngineRemote;
import com.orientechnologies.orient.client.remote.OServerAdmin;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.db.record.ridbag.RidBag;
import com.orientechnologies.orient.core.engine.memory.OEngineMemory;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.storage.cache.local.OWOWCache;
import com.orientechnologies.orient.core.storage.disk.OLocalPaginatedStorage;
import com.orientechnologies.orient.core.storage.ridbag.sbtree.OSBTreeCollectionManagerShared;
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
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    bottomThreshold =
        YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.getValueAsInteger();

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, -1);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, -1);
      server.close();
    }

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(-1);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(-1);
  }

  @AfterMethod
  public void afterMethod() throws IOException {
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(topThreshold);
    YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD.setValue(bottomThreshold);

    if (database.isRemote()) {
      OServerAdmin server =
          new OServerAdmin(database.getURL())
              .connect("root", SERVER_PASSWORD);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD, topThreshold);
      server.setGlobalConfiguration(
          YTGlobalConfiguration.RID_BAG_SBTREEBONSAI_TO_EMBEDDED_THRESHOLD, bottomThreshold);
      server.close();
    }
  }

  public void testRidBagClusterDistribution() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) {
      return;
    }

    final int clusterIdOne = database.addCluster("clusterOne");

    YTEntityImpl docClusterOne = new YTEntityImpl();
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
    YTEntityImpl scuti =
        new YTEntityImpl()
            .field("name", "UY Scuti");
    scuti.save(database.getClusterNameById(database.getDefaultClusterId()));
    YTEntityImpl cygni =
        new YTEntityImpl()
            .field("name", "NML Cygni");
    cygni.save(database.getClusterNameById(database.getDefaultClusterId()));
    YTEntityImpl scorpii =
        new YTEntityImpl()
            .field("name", "AH Scorpii");
    scorpii.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    scuti = database.bindToSession(scuti);
    cygni = database.bindToSession(cygni);
    scorpii = database.bindToSession(scorpii);

    HashSet<YTEntityImpl> expectedResult = new HashSet<YTEntityImpl>(Arrays.asList(scuti, scorpii));

    RidBag bag = new RidBag(database);
    bag.add(scuti);
    bag.add(cygni);
    bag.add(scorpii);

    YTEntityImpl doc = new YTEntityImpl();
    doc.field("ridBag", bag);

    database.begin();
    doc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    doc = database.bindToSession(doc);
    bag = doc.field("ridBag");
    bag.remove(cygni);

    Set<YTEntityImpl> result = new HashSet<YTEntityImpl>();
    for (YTIdentifiable identifiable : bag) {
      result.add(identifiable.getRecord());
    }

    Assert.assertEquals(result, expectedResult);
  }

  public void testRidBagConversion() {
    final int oldThreshold =
        YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.getValueAsInteger();
    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(5);

    database.begin();
    YTEntityImpl doc_1 = new YTEntityImpl();
    doc_1.save(database.getClusterNameById(database.getDefaultClusterId()));

    YTEntityImpl doc_2 = new YTEntityImpl();
    doc_2.save(database.getClusterNameById(database.getDefaultClusterId()));

    YTEntityImpl doc_3 = new YTEntityImpl();
    doc_3.save(database.getClusterNameById(database.getDefaultClusterId()));

    YTEntityImpl doc_4 = new YTEntityImpl();
    doc_4.save(database.getClusterNameById(database.getDefaultClusterId()));

    YTEntityImpl doc = new YTEntityImpl();

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
    YTEntityImpl doc_5 = new YTEntityImpl();
    doc_5.save(database.getClusterNameById(database.getDefaultClusterId()));

    YTEntityImpl doc_6 = new YTEntityImpl();
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

    YTGlobalConfiguration.RID_BAG_EMBEDDED_TO_SBTREEBONSAI_THRESHOLD.setValue(oldThreshold);
    database.rollback();
  }

  public void testRidBagDelete() {
    if (database.getStorage().getType().equals(OEngineRemote.NAME)
        || database.getStorage().getType().equals(OEngineMemory.NAME)) {
      return;
    }

    float reuseTrigger =
        YTGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.getValueAsFloat();
    YTGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(Float.MIN_VALUE);

    YTEntityImpl realDoc = new YTEntityImpl();
    RidBag realDocRidBag = new RidBag(database);
    realDoc.field("ridBag", realDocRidBag);

    for (int i = 0; i < 10; i++) {
      YTEntityImpl docToAdd = new YTEntityImpl();
      realDocRidBag.add(docToAdd);
    }

    assertEmbedded(realDocRidBag.isEmbedded());

    database.begin();
    realDoc.save(database.getClusterNameById(database.getDefaultClusterId()));
    database.commit();

    final int clusterId = database.addCluster("ridBagDeleteTest");

    YTEntityImpl testDocument = crateTestDeleteDoc(realDoc);
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

    YTGlobalConfiguration.SBTREEBOSAI_FREE_SPACE_REUSE_TRIGGER.setValue(reuseTrigger);
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

  private YTEntityImpl crateTestDeleteDoc(YTEntityImpl realDoc) {
    YTEntityImpl testDocument = new YTEntityImpl();
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
