/*
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 */

package com.jetbrains.youtrack.db.internal.core;

import com.jetbrains.youtrack.db.internal.common.util.OCallable;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.command.OCommandOutputListener;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestText;
import com.jetbrains.youtrack.db.internal.core.config.YTContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.conflict.ORecordConflictStrategy;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.db.YouTrackDBInternal;
import com.jetbrains.youtrack.db.internal.core.db.document.YTDatabaseDocumentTx;
import com.jetbrains.youtrack.db.internal.core.db.record.OCurrentStorageComponentsFactory;
import com.jetbrains.youtrack.db.internal.core.db.record.ORecordOperation;
import com.jetbrains.youtrack.db.internal.core.engine.OEngine;
import com.jetbrains.youtrack.db.internal.core.engine.OEngineAbstract;
import com.jetbrains.youtrack.db.internal.core.id.YTRID;
import com.jetbrains.youtrack.db.internal.core.id.YTRecordId;
import com.jetbrains.youtrack.db.internal.core.storage.OCluster;
import com.jetbrains.youtrack.db.internal.core.storage.OPhysicalPosition;
import com.jetbrains.youtrack.db.internal.core.storage.ORawBuffer;
import com.jetbrains.youtrack.db.internal.core.storage.ORecordCallback;
import com.jetbrains.youtrack.db.internal.core.storage.ORecordMetadata;
import com.jetbrains.youtrack.db.internal.core.storage.OStorage;
import com.jetbrains.youtrack.db.internal.core.storage.cluster.OPaginatedCluster;
import com.jetbrains.youtrack.db.internal.core.storage.config.OClusterBasedStorageConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.ridbag.sbtree.OSBTreeCollectionManager;
import com.jetbrains.youtrack.db.internal.core.tx.OTransactionOptimistic;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
import java.util.concurrent.Callable;
import javax.annotation.Nonnull;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 *
 */
public class PostponedEngineStartTest {

  private static YouTrackDBManager YOU_TRACK;

  private static OEngine ENGINE1;
  private static OEngine ENGINE2;
  private static OEngine FAULTY_ENGINE;

  @BeforeClass
  public static void before() {
    YOU_TRACK =
        new YouTrackDBManager(false) {
          @Override
          public YouTrackDBManager startup() {
            YOU_TRACK.registerEngine(ENGINE1 = new NamedEngine("engine1"));
            YOU_TRACK.registerEngine(ENGINE2 = new NamedEngine("engine2"));
            YOU_TRACK.registerEngine(FAULTY_ENGINE = new FaultyEngine());
            return this;
          }

          @Override
          public YouTrackDBManager shutdown() {
            YTDatabaseDocumentTx.closeAll();
            return this;
          }
        };

    YOU_TRACK.startup();
  }

  @Test
  public void test() {
    // XXX: There is a known problem in TestNG runner with hardly controllable test methods
    // interleaving from different
    // test classes. This test case touches internals of YouTrackDB runtime, interleaving with foreign
    // methods is not acceptable
    // here. So I just invoke "test" methods manually from a single test method.
    //
    // BTW, TestNG author says that is not a problem, we just need to split *ALL* our test classes
    // into groups and
    // make groups depend on each other in right order. I see many problems here: (a) we have to to
    // split into groups,
    // (b) we have to maintain all that zoo and (c) we lose the ability to run each test case
    // individually since
    // group dependency must be run before.

    testEngineShouldNotStartAtRuntimeStart();
    testGetEngineIfRunningShouldReturnNullEngineIfNotRunning();
    testGetRunningEngineShouldStartEngine();
    testEngineRestart();
    testStoppedEngineShouldStartAndCreateStorage();
    testGetRunningEngineShouldThrowIfEngineIsUnknown();
    testGetRunningEngineShouldThrowIfEngineIsUnableToStart();
  }

  // @Test
  public void testEngineShouldNotStartAtRuntimeStart() {
    final OEngine engine = YOU_TRACK.getEngine(ENGINE1.getName());
    Assert.assertFalse(engine.isRunning());
  }

  // @Test(dependsOnMethods = "testEngineShouldNotStartAtRuntimeStart")
  public void testGetEngineIfRunningShouldReturnNullEngineIfNotRunning() {
    final OEngine engine = YOU_TRACK.getEngineIfRunning(ENGINE1.getName());
    Assert.assertNull(engine);
  }

  // @Test(dependsOnMethods = "testGetEngineIfRunningShouldReturnNullEngineIfNotRunning")
  public void testGetRunningEngineShouldStartEngine() {
    final OEngine engine = YOU_TRACK.getRunningEngine(ENGINE1.getName());
    Assert.assertNotNull(engine);
    Assert.assertTrue(engine.isRunning());
  }

  // @Test(dependsOnMethods = "testGetRunningEngineShouldStartEngine")
  public void testEngineRestart() {
    OEngine engine = YOU_TRACK.getRunningEngine(ENGINE1.getName());
    engine.shutdown();
    Assert.assertFalse(engine.isRunning());

    engine = YOU_TRACK.getEngineIfRunning(ENGINE1.getName());
    Assert.assertNull(engine);

    engine = YOU_TRACK.getEngine(ENGINE1.getName());
    Assert.assertFalse(engine.isRunning());

    engine = YOU_TRACK.getRunningEngine(ENGINE1.getName());
    Assert.assertTrue(engine.isRunning());
  }

  // @Test
  public void testStoppedEngineShouldStartAndCreateStorage() {
    OEngine engine = YOU_TRACK.getEngineIfRunning(ENGINE2.getName());
    Assert.assertNull(engine);

    final OStorage storage =
        ENGINE2.createStorage(
            ENGINE2.getName() + ":storage",
            125 * 1024 * 1024,
            25 * 1024 * 1024,
            Integer.MAX_VALUE,
            null);

    Assert.assertNotNull(storage);

    engine = YOU_TRACK.getRunningEngine(ENGINE2.getName());
    Assert.assertTrue(engine.isRunning());
  }

  //  @Test(expected = IllegalStateException.class)
  public void testGetRunningEngineShouldThrowIfEngineIsUnknown() {
    //    Assert.assertThrows(IllegalStateException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        ORIENT.getRunningEngine("unknown engine");
    //      }
    //    });
    try {
      YOU_TRACK.getRunningEngine("unknown engine");
      Assert.fail();
    } catch (Exception e) {
      // exception expected
    }
  }

  // @Test(expected = IllegalStateException.class)
  public void testGetRunningEngineShouldThrowIfEngineIsUnableToStart() {
    OEngine engine = YOU_TRACK.getEngine(FAULTY_ENGINE.getName());
    Assert.assertNotNull(engine);

    //    Assert.assertThrows(IllegalStateException.class, new Assert.ThrowingRunnable() {
    //      @Override
    //      public void run() throws Throwable {
    //        ORIENT.getRunningEngine(FAULTY_ENGINE.getName());
    //      }
    //    });
    try {

      YOU_TRACK.getRunningEngine(FAULTY_ENGINE.getName());

      engine = YOU_TRACK.getEngine(FAULTY_ENGINE.getName());
      Assert.assertNull(engine);
      Assert.fail();
    } catch (Exception e) {
      // exception expected
    }
  }

  private static class NamedEngine extends OEngineAbstract {

    private final String name;

    public NamedEngine(String name) {
      this.name = name;
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public OStorage createStorage(
        String iURL,
        long maxWalSegSize,
        long doubleWriteLogMaxSegSize,
        int storageId,
        YouTrackDBInternal context) {
      return new OStorage() {

        @Override
        public List<String> backup(
            OutputStream out,
            Map<String, Object> options,
            Callable<Object> callable,
            OCommandOutputListener iListener,
            int compressionLevel,
            int bufferSize) {
          return null;
        }

        @Override
        public void restore(
            InputStream in,
            Map<String, Object> options,
            Callable<Object> callable,
            OCommandOutputListener iListener) {
        }

        @Override
        public String getClusterName(YTDatabaseSessionInternal database, int clusterId) {
          return null;
        }

        @Override
        public boolean setClusterAttribute(int id, OCluster.ATTRIBUTES attribute, Object value) {
          return false;
        }

        @Override
        public String getCreatedAtVersion() {
          return null;
        }

        @Override
        public void open(
            YTDatabaseSessionInternal remote, String iUserName, String iUserPassword,
            YTContextConfiguration contextConfiguration) {
        }

        @Override
        public void create(YTContextConfiguration contextConfiguration) {
        }

        @Override
        public boolean exists() {
          return false;
        }

        @Override
        public void reload(YTDatabaseSessionInternal database) {
        }

        @Override
        public void delete() {
        }

        @Override
        public void close(YTDatabaseSessionInternal session) {
        }

        @Override
        public void close(YTDatabaseSessionInternal database, boolean iForce) {
        }

        @Override
        public boolean isClosed(YTDatabaseSessionInternal database) {
          return false;
        }

        @Override
        public @Nonnull ORawBuffer readRecord(
            YTDatabaseSessionInternal session, YTRecordId iRid,
            boolean iIgnoreCache,
            boolean prefetchRecords,
            ORecordCallback<ORawBuffer> iCallback) {
          return null;
        }

        @Override
        public boolean recordExists(YTDatabaseSessionInternal session, YTRID rid) {
          return false;
        }

        @Override
        public ORecordMetadata getRecordMetadata(YTDatabaseSessionInternal session, YTRID rid) {
          return null;
        }

        @Override
        public boolean cleanOutRecord(
            YTDatabaseSessionInternal session, YTRecordId recordId, int recordVersion, int iMode,
            ORecordCallback<Boolean> callback) {
          return false;
        }

        @Override
        public List<ORecordOperation> commit(OTransactionOptimistic iTx) {
          return null;
        }

        @Override
        public OClusterBasedStorageConfiguration getConfiguration() {
          return null;
        }

        @Override
        public int getClusters() {
          return 0;
        }

        @Override
        public Set<String> getClusterNames() {
          return null;
        }

        @Override
        public Collection<? extends OCluster> getClusterInstances() {
          return null;
        }

        @Override
        public int addCluster(YTDatabaseSessionInternal database, String iClusterName,
            Object... iParameters) {
          return 0;
        }

        @Override
        public int addCluster(YTDatabaseSessionInternal database, String iClusterName,
            int iRequestedId) {
          return 0;
        }

        @Override
        public boolean dropCluster(YTDatabaseSessionInternal session, String iClusterName) {
          return false;
        }

        @Override
        public boolean dropCluster(YTDatabaseSessionInternal database, int iId) {
          return false;
        }

        @Override
        public String getClusterNameById(int clusterId) {
          return null;
        }

        @Override
        public long getClusterRecordsSizeById(int clusterId) {
          return 0;
        }

        @Override
        public long getClusterRecordsSizeByName(String clusterName) {
          return 0;
        }

        @Override
        public String getClusterRecordConflictStrategy(int clusterId) {
          return null;
        }

        @Override
        public String getClusterEncryption(int clusterId) {
          return null;
        }

        @Override
        public boolean isSystemCluster(int clusterId) {
          return false;
        }

        @Override
        public long getLastClusterPosition(int clusterId) {
          return 0;
        }

        @Override
        public long getClusterNextPosition(int clusterId) {
          return 0;
        }

        @Override
        public OPaginatedCluster.RECORD_STATUS getRecordStatus(YTRID rid) {
          return null;
        }

        @Override
        public long count(YTDatabaseSessionInternal session, int iClusterId) {
          return 0;
        }

        @Override
        public long count(YTDatabaseSessionInternal session, int iClusterId,
            boolean countTombstones) {
          return 0;
        }

        @Override
        public long count(YTDatabaseSessionInternal session, int[] iClusterIds) {
          return 0;
        }

        @Override
        public long count(YTDatabaseSessionInternal session, int[] iClusterIds,
            boolean countTombstones) {
          return 0;
        }

        @Override
        public long getSize(YTDatabaseSessionInternal session) {
          return 0;
        }

        @Override
        public long countRecords(YTDatabaseSessionInternal session) {
          return 0;
        }

        @Override
        public int getDefaultClusterId() {
          return 0;
        }

        @Override
        public void setDefaultClusterId(int defaultClusterId) {
        }

        @Override
        public int getClusterIdByName(String iClusterName) {
          return 0;
        }

        @Override
        public String getPhysicalClusterNameById(int iClusterId) {
          return null;
        }

        @Override
        public boolean checkForRecordValidity(OPhysicalPosition ppos) {
          return false;
        }

        @Override
        public String getName() {
          return null;
        }

        @Override
        public String getURL() {
          return null;
        }

        @Override
        public long getVersion() {
          return 0;
        }

        @Override
        public void synch() {
        }

        @Override
        public Object command(YTDatabaseSessionInternal database, OCommandRequestText iCommand) {
          return null;
        }

        @Override
        public long[] getClusterDataRange(YTDatabaseSessionInternal session, int currentClusterId) {
          return new long[0];
        }

        @Override
        public OPhysicalPosition[] higherPhysicalPositions(
            YTDatabaseSessionInternal session, int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] lowerPhysicalPositions(
            YTDatabaseSessionInternal session, int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] ceilingPhysicalPositions(
            YTDatabaseSessionInternal session, int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public OPhysicalPosition[] floorPhysicalPositions(
            YTDatabaseSessionInternal session, int clusterId, OPhysicalPosition physicalPosition) {
          return new OPhysicalPosition[0];
        }

        @Override
        public STATUS getStatus() {
          return null;
        }

        @Override
        public String getType() {
          return null;
        }

        @Override
        public OStorage getUnderlying() {
          return null;
        }

        @Override
        public boolean isRemote() {
          return false;
        }

        @Override
        public boolean isDistributed() {
          return false;
        }

        @Override
        public boolean isAssigningClusterIds() {
          return false;
        }

        @Override
        public OSBTreeCollectionManager getSBtreeCollectionManager() {
          return null;
        }

        @Override
        public OCurrentStorageComponentsFactory getComponentsFactory() {
          return null;
        }

        @Override
        public ORecordConflictStrategy getRecordConflictStrategy() {
          return null;
        }

        @Override
        public void setConflictStrategy(ORecordConflictStrategy iResolver) {
        }

        @Override
        public String incrementalBackup(YTDatabaseSessionInternal session, String backupDirectory,
            OCallable<Void, Void> started) {
          return null;
        }

        @Override
        public boolean supportIncremental() {
          return false;
        }

        @Override
        public void fullIncrementalBackup(final OutputStream stream)
            throws UnsupportedOperationException {
        }

        @Override
        public void restoreFromIncrementalBackup(YTDatabaseSessionInternal session,
            String filePath) {
        }

        @Override
        public void restoreFullIncrementalBackup(YTDatabaseSessionInternal session,
            final InputStream stream)
            throws UnsupportedOperationException {
        }

        @Override
        public void shutdown() {
        }

        @Override
        public void setSchemaRecordId(String schemaRecordId) {
        }

        @Override
        public void setDateFormat(String dateFormat) {
        }

        @Override
        public void setTimeZone(TimeZone timeZoneValue) {
        }

        @Override
        public void setLocaleLanguage(String locale) {
        }

        @Override
        public void setCharset(String charset) {
        }

        @Override
        public void setIndexMgrRecordId(String indexMgrRecordId) {
        }

        @Override
        public void setDateTimeFormat(String dateTimeFormat) {
        }

        @Override
        public void setLocaleCountry(String localeCountry) {
        }

        @Override
        public void setClusterSelection(String clusterSelection) {
        }

        @Override
        public void setMinimumClusters(int minimumClusters) {
        }

        @Override
        public void setValidation(boolean validation) {
        }

        @Override
        public void removeProperty(String property) {
        }

        @Override
        public void setProperty(String property, String value) {
        }

        @Override
        public void setRecordSerializer(String recordSerializer, int version) {
        }

        @Override
        public void clearProperties() {
        }

        @Override
        public int[] getClustersIds(Set<String> filterClusters) {
          return null;
        }

        @Override
        public YouTrackDBInternal getContext() {
          return null;
        }
      };
    }

    @Override
    public String getNameFromPath(String dbPath) {
      return dbPath;
    }
  }

  private static class FaultyEngine extends OEngineAbstract {

    @Override
    public String getName() {
      return FaultyEngine.class.getSimpleName();
    }

    @Override
    public void startup() {
      super.startup();
      throw new RuntimeException("oops");
    }

    @Override
    public OStorage createStorage(
        String iURL,
        long maxWalSegSize,
        long doubleWriteLogMaxSegSize,
        int storageId,
        YouTrackDBInternal context) {
      throw new UnsupportedOperationException();
    }

    @Override
    public String getNameFromPath(String dbPath) {
      throw new UnsupportedOperationException();
    }
  }
}
