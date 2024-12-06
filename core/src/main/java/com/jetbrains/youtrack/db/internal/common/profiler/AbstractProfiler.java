/*
 *
 *
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
 *
 */

package com.jetbrains.youtrack.db.internal.common.profiler;

import com.jetbrains.youtrack.db.internal.common.concur.resource.SharedResourceAbstract;
import com.jetbrains.youtrack.db.internal.common.io.FileUtils;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBManager;
import com.jetbrains.youtrack.db.internal.core.YouTrackDBStartupListener;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.storage.Storage;
import com.jetbrains.youtrack.db.internal.core.storage.cache.ReadCache;
import com.jetbrains.youtrack.db.internal.core.storage.cache.WriteCache;
import com.jetbrains.youtrack.db.internal.core.storage.disk.LocalPaginatedStorage;
import java.io.File;
import java.io.PrintStream;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.management.MBeanServer;
import javax.management.ObjectName;

public abstract class AbstractProfiler extends SharedResourceAbstract
    implements Profiler, YouTrackDBStartupListener, ProfilerMXBean {

  protected final Map<String, ProfilerHookRuntime> hooks =
      new ConcurrentHashMap<String, ProfilerHookRuntime>();
  protected final ConcurrentHashMap<String, String> dictionary =
      new ConcurrentHashMap<String, String>();
  protected final ConcurrentHashMap<String, METRIC_TYPE> types =
      new ConcurrentHashMap<String, METRIC_TYPE>();
  protected long recordingFrom = -1;
  protected TimerTask autoDumpTask;
  protected List<ProfilerListener> listeners = new ArrayList<ProfilerListener>();

  private static long statsCreateRecords = 0;
  private static long statsReadRecords = 0;
  private static long statsUpdateRecords = 0;
  private static long statsDeleteRecords = 0;
  private static long statsCommands = 0;
  private static long statsTxCommit = 0;
  private static long statsTxRollback = 0;
  private static long statsLastAutoDump = 0;

  public interface ProfilerHookValue {

    Object getValue();
  }

  public class ProfilerHookRuntime {

    public ProfilerHookValue hook;
    public METRIC_TYPE type;

    public ProfilerHookRuntime(final ProfilerHookValue hook, final METRIC_TYPE type) {
      this.hook = hook;
      this.type = type;
    }
  }

  public class ProfilerHookStatic {

    public Object value;
    public METRIC_TYPE type;

    public ProfilerHookStatic(final Object value, final METRIC_TYPE type) {
      this.value = value;
      this.type = type;
    }
  }

  private static final class MemoryChecker implements Runnable {

    @Override
    public void run() {
      try {
        final long jvmTotMemory = Runtime.getRuntime().totalMemory();
        final long jvmMaxMemory = Runtime.getRuntime().maxMemory();

        for (Storage s : YouTrackDBManager.instance().getStorages()) {
          if (s instanceof LocalPaginatedStorage) {
            final ReadCache dk = ((LocalPaginatedStorage) s).getReadCache();
            final WriteCache wk = ((LocalPaginatedStorage) s).getWriteCache();
            if (dk == null || wk == null)
            // NOT YET READY
            {
              continue;
            }

            final long totalDiskCacheUsedMemory =
                (dk.getUsedMemory() + wk.getExclusiveWriteCachePagesSize()) / FileUtils.MEGABYTE;
            final long maxDiskCacheUsedMemory =
                GlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong();

            // CHECK IF THERE IS MORE THAN 40% HEAP UNUSED AND DISK-CACHE IS 80% OF THE MAXIMUM SIZE
            if ((jvmTotMemory * 140 / 100) < jvmMaxMemory
                && (totalDiskCacheUsedMemory * 120 / 100) > maxDiskCacheUsedMemory) {

              final long suggestedMaxHeap = jvmTotMemory * 120 / 100;
              final long suggestedDiskCache =
                  GlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong()
                      + (jvmMaxMemory - suggestedMaxHeap) / FileUtils.MEGABYTE;

              LogManager.instance()
                  .info(
                      this,
                      "Database '%s' uses %,dMB/%,dMB of DISKCACHE memory, while Heap is not"
                          + " completely used (usedHeap=%dMB maxHeap=%dMB). To improve performance"
                          + " set maxHeap to %dMB and DISKCACHE to %dMB",
                      s.getName(),
                      totalDiskCacheUsedMemory,
                      maxDiskCacheUsedMemory,
                      jvmTotMemory / FileUtils.MEGABYTE,
                      jvmMaxMemory / FileUtils.MEGABYTE,
                      suggestedMaxHeap / FileUtils.MEGABYTE,
                      suggestedDiskCache);

              LogManager.instance()
                  .info(
                      this,
                      "-> Open server.sh (or server.bat on Windows) and change the following"
                          + " variables: 1) MAXHEAP=-Xmx%dM 2) MAXDISKCACHE=%d",
                      suggestedMaxHeap / FileUtils.MEGABYTE,
                      suggestedDiskCache);
            }
          }
        }
      } catch (Exception e) {
        LogManager.instance().debug(this, "Error on memory checker task", e);
      } catch (Error e) {
        LogManager.instance().debug(this, "Error on memory checker task", e);
        throw e;
      }
    }
  }

  public AbstractProfiler() {
    this(true);
  }

  public AbstractProfiler(boolean registerListener) {
    if (registerListener) {
      YouTrackDBManager.instance().registerWeakYouTrackDBStartupListener(this);
    }
  }

  public AbstractProfiler(final AbstractProfiler profiler) {
    hooks.putAll(profiler.hooks);
    dictionary.putAll(profiler.dictionary);
    types.putAll(profiler.types);

    YouTrackDBManager.instance().registerWeakYouTrackDBStartupListener(this);
  }

  protected abstract void setTip(String iMessage, AtomicInteger counter);

  protected abstract AtomicInteger getTip(String iMessage);

  public static String dumpEnvironment(final String dumpType) {
    final StringBuilder buffer = new StringBuilder();

    final Runtime runtime = Runtime.getRuntime();

    final long freeSpaceInMB = new File(".").getFreeSpace();
    final long totalSpaceInMB = new File(".").getTotalSpace();

    int stgs = 0;
    long diskCacheUsed = 0;
    long diskCacheTotal = 0;
    for (Storage stg : YouTrackDBManager.instance().getStorages()) {
      if (stg instanceof LocalPaginatedStorage) {
        diskCacheUsed += ((LocalPaginatedStorage) stg).getReadCache().getUsedMemory();
        diskCacheTotal += GlobalConfiguration.DISK_CACHE_SIZE.getValueAsLong() * 1024 * 1024;
        stgs++;
      }
    }
    try {
      MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
      ObjectName osMBeanName =
          ObjectName.getInstance(ManagementFactory.OPERATING_SYSTEM_MXBEAN_NAME);
      if (mbs.isInstanceOf(osMBeanName, "com.sun.management.OperatingSystemMXBean")) {
        final long osTotalMem =
            ((Number) mbs.getAttribute(osMBeanName, "TotalPhysicalMemorySize")).longValue();
        final long osUsedMem =
            osTotalMem
                - ((Number) mbs.getAttribute(osMBeanName, "FreePhysicalMemorySize")).longValue();

        buffer.append(
            String.format(
                "YouTrackDB Memory profiler: HEAP=%s of %s - DISKCACHE (%s dbs)=%s of %s - OS=%s of"
                    + " %s - FS=%s of %s",
                FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
                FileUtils.getSizeAsString(runtime.maxMemory()),
                stgs,
                FileUtils.getSizeAsString(diskCacheUsed),
                FileUtils.getSizeAsString(diskCacheTotal),
                FileUtils.getSizeAsString(osUsedMem),
                FileUtils.getSizeAsString(osTotalMem),
                FileUtils.getSizeAsString(freeSpaceInMB),
                FileUtils.getSizeAsString(totalSpaceInMB)));
      }

    } catch (Exception e) {
      // JMX NOT AVAILABLE, AVOID OS DATA
      buffer.append(
          String.format(
              "YouTrackDB Memory profiler: HEAP=%s of %s - DISKCACHE (%s dbs)=%s of %s - FS=%s of %s",
              FileUtils.getSizeAsString(runtime.totalMemory() - runtime.freeMemory()),
              FileUtils.getSizeAsString(runtime.maxMemory()),
              stgs,
              FileUtils.getSizeAsString(diskCacheUsed),
              FileUtils.getSizeAsString(diskCacheTotal),
              FileUtils.getSizeAsString(freeSpaceInMB),
              FileUtils.getSizeAsString(totalSpaceInMB)));
    }

    if ("performance".equalsIgnoreCase(dumpType)) {
      try {
        long lastCreateRecords = 0;
        long lastReadRecords = 0;
        long lastUpdateRecords = 0;
        long lastDeleteRecords = 0;
        long lastCommands = 0;
        long lastTxCommit = 0;
        long lastTxRollback = 0;

        if (statsLastAutoDump > 0) {
          final long msFromLastDump = System.currentTimeMillis() - statsLastAutoDump;

          final String[] hooks = YouTrackDBManager.instance().getProfiler().getHookAsString();
          for (String h : hooks) {
            if (h.startsWith("db.") && h.endsWith("createRecord")) {
              lastCreateRecords += (Long) YouTrackDBManager.instance().getProfiler()
                  .getHookValue(h);
            } else if (h.startsWith("db.") && h.endsWith("readRecord")) {
              lastReadRecords += (Long) YouTrackDBManager.instance().getProfiler().getHookValue(h);
            } else if (h.startsWith("db.") && h.endsWith("updateRecord")) {
              lastUpdateRecords += (Long) YouTrackDBManager.instance().getProfiler()
                  .getHookValue(h);
            } else if (h.startsWith("db.") && h.endsWith("deleteRecord")) {
              lastDeleteRecords += (Long) YouTrackDBManager.instance().getProfiler()
                  .getHookValue(h);
            } else if (h.startsWith("db.") && h.endsWith("txCommit")) {
              lastTxCommit += (Long) YouTrackDBManager.instance().getProfiler().getHookValue(h);
            } else if (h.startsWith("db.") && h.endsWith("txRollback")) {
              lastTxRollback += (Long) YouTrackDBManager.instance().getProfiler().getHookValue(h);
            }
          }

          final List<String> chronos = YouTrackDBManager.instance().getProfiler().getChronos();
          for (String c : chronos) {
            final ProfilerEntry chrono = YouTrackDBManager.instance().getProfiler().getChrono(c);
            if (chrono != null) {
              if (c.startsWith("db.") && c.contains(".command.")) {
                lastCommands += chrono.entries;
              }
            }
          }

          long lastCreateRecordsSec;
          if (lastCreateRecords == 0) {
            lastCreateRecordsSec = msFromLastDump < 1000 ? 1 : 0;
          } else {
            lastCreateRecordsSec =
                (lastCreateRecords - statsCreateRecords) / (msFromLastDump / 1000);
          }

          long lastReadRecordsSec;
          if (msFromLastDump < 1000) {
            lastReadRecordsSec = lastReadRecords == 0 ? 0 : 1;
          } else if (lastReadRecords == 0) {
            lastReadRecordsSec = 0;
          } else {
            lastReadRecordsSec = (lastReadRecords - statsReadRecords) / (msFromLastDump / 1000);
          }

          long lastUpdateRecordsSec;
          if (lastUpdateRecords == 0 || msFromLastDump < 1000) {
            lastUpdateRecordsSec = 0;
          } else {
            lastUpdateRecordsSec =
                (lastUpdateRecords - statsUpdateRecords) / (msFromLastDump / 1000);
          }

          long lastDeleteRecordsSec;
          if (lastDeleteRecords == 0) {
            lastDeleteRecordsSec = 0;
          } else if (msFromLastDump < 1000) {
            lastDeleteRecordsSec = 1;
          } else {
            lastDeleteRecordsSec =
                (lastDeleteRecords - statsDeleteRecords) / (msFromLastDump / 1000);
          }

          long lastCommandsSec;
          if (lastCommands == 0) {
            lastCommandsSec = 0;
          } else if (msFromLastDump < 1000) {
            lastCommandsSec = 1;
          } else {
            lastCommandsSec = (lastCommands - statsCommands) / (msFromLastDump / 1000);
          }

          long lastTxCommitSec;
          if (lastTxCommit == 0) {
            lastTxCommitSec = 0;
          } else if (msFromLastDump < 1000) {
            lastTxCommitSec = 1;
          } else {
            lastTxCommitSec = (lastTxCommit - statsTxCommit) / (msFromLastDump / 1000);
          }

          long lastTxRollbackSec;
          if (lastTxRollback == 0) {
            lastTxRollbackSec = 0;
          } else if (msFromLastDump < 1000) {
            lastTxRollbackSec = 1;
          } else {
            lastTxRollbackSec = (lastTxRollback - statsTxRollback) / (msFromLastDump / 1000);
          }

          buffer.append(
              String.format(
                  "\n"
                      + "CRUD: C(%d %d/sec) R(%d %d/sec) U(%d %d/sec) D(%d %d/sec) - COMMANDS (%d"
                      + " %d/sec) - TX: COMMIT(%d %d/sec) ROLLBACK(%d %d/sec)",
                  lastCreateRecords,
                  lastCreateRecordsSec,
                  lastReadRecords,
                  lastReadRecordsSec,
                  lastUpdateRecords,
                  lastUpdateRecordsSec,
                  lastDeleteRecords,
                  lastDeleteRecordsSec,
                  lastCommands,
                  lastCommandsSec,
                  lastTxCommit,
                  lastTxCommitSec,
                  lastTxRollback,
                  lastTxRollbackSec));
        }

        statsLastAutoDump = System.currentTimeMillis();
        statsCreateRecords = lastCreateRecords;
        statsReadRecords = lastReadRecords;
        statsUpdateRecords = lastUpdateRecords;
        statsDeleteRecords = lastDeleteRecords;
        statsCommands = lastCommands;
        statsTxCommit = lastTxCommit;
        statsTxRollback = lastTxRollback;

      } catch (Exception e) {
        // IGNORE IT
      }
    }

    return buffer.toString();
  }

  @Override
  public void onStartup() {
    if (GlobalConfiguration.PROFILER_ENABLED.getValueAsBoolean())
    // ACTIVATE RECORDING OF THE PROFILER
    {
      startRecording();
    }
    installMemoryChecker();
  }

  public void shutdown() {
    stopRecording();
  }

  public int reportTip(final String iMessage) {
    AtomicInteger counter = getTip(iMessage);
    if (counter == null) {
      // DUMP THE MESSAGE ONLY THE FIRST TIME
      LogManager.instance().info(this, "[TIP] " + iMessage);

      counter = new AtomicInteger(0);
    }

    setTip(iMessage, counter);

    return counter.incrementAndGet();
  }

  public boolean startRecording() {
    if (isRecording()) {
      return false;
    }

    recordingFrom = System.currentTimeMillis();
    return true;
  }

  public boolean stopRecording() {
    if (!isRecording()) {
      return false;
    }

    recordingFrom = -1;
    return true;
  }

  public boolean isRecording() {
    return recordingFrom > -1;
  }

  public void updateCounter(final String iStatName, final String iDescription, final long iPlus) {
    updateCounter(iStatName, iDescription, iPlus, iStatName);
  }

  @Override
  public String getName() {
    return "profiler";
  }

  @Override
  public void startup() {
    startRecording();
  }

  @Override
  public String dump() {
    return dumpEnvironment(GlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString());
  }

  @Override
  public void dump(final PrintStream out) {
    out.println(dumpEnvironment(GlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString()));
  }

  @Override
  public String dump(final String type) {
    return dumpEnvironment(type);
  }

  @Override
  public String dumpCounters() {
    return null;
  }

  @Override
  public ProfilerEntry getChrono(String string) {
    return null;
  }

  @Override
  public long startChrono() {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime) {
    return 0;
  }

  @Override
  public long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary) {
    return 0;
  }

  @Override
  public String dumpChronos() {
    return null;
  }

  @Override
  public String[] getCountersAsString() {
    return null;
  }

  @Override
  public String[] getChronosAsString() {
    return null;
  }

  @Override
  public Date getLastReset() {
    return null;
  }

  @Override
  public void setAutoDump(final int iSeconds) {
    if (autoDumpTask != null) {
      // CANCEL ANY PREVIOUS RUNNING TASK
      autoDumpTask.cancel();
      autoDumpTask = null;
    }

    if (iSeconds > 0) {
      LogManager.instance()
          .info(this, "Enabled auto dump of profiler every %d second(s)", iSeconds);

      final int ms = iSeconds * 1000;

      autoDumpTask =
          YouTrackDBManager.instance()
              .scheduleTask(
                  () -> {
                    final StringBuilder output = new StringBuilder();

                    final String dumpType =
                        GlobalConfiguration.PROFILER_AUTODUMP_TYPE.getValueAsString();

                    output.append(
                        "\n"
                            + "*******************************************************************************************************************************************");
                    output.append(
                        "\nPROFILER AUTO DUMP '"
                            + dumpType
                            + "' OUTPUT (to disabled it set 'profiler.autoDump.interval' = 0):\n");
                    output.append(dump(dumpType));
                    output.append(
                        "\n"
                            + "*******************************************************************************************************************************************");

                    LogManager.instance().info(this, output.toString());
                  },
                  ms,
                  ms);
    } else {
      LogManager.instance().info(this, "Auto dump of profiler disabled", iSeconds);
    }
  }

  @Override
  public String metadataToJSON() {
    return null;
  }

  @Override
  public Map<String, Pair<String, METRIC_TYPE>> getMetadata() {
    final Map<String, Pair<String, METRIC_TYPE>> metadata =
        new HashMap<String, Pair<String, METRIC_TYPE>>();
    for (Entry<String, String> entry : dictionary.entrySet()) {
      metadata.put(
          entry.getKey(),
          new Pair<String, METRIC_TYPE>(entry.getValue(), types.get(entry.getKey())));
    }
    return metadata;
  }

  @Override
  public String[] getHookAsString() {
    final List<String> keys = new ArrayList<String>(hooks.keySet());
    final String[] array = new String[keys.size()];
    return keys.toArray(array);
  }

  public void registerHookValue(
      final String iName,
      final String iDescription,
      final METRIC_TYPE iType,
      final ProfilerHookValue iHookValue) {
    registerHookValue(iName, iDescription, iType, iHookValue, iName);
  }

  public void registerHookValue(
      final String iName,
      final String iDescription,
      final METRIC_TYPE iType,
      final ProfilerHookValue iHookValue,
      final String iMetadataName) {
    if (iName != null) {
      unregisterHookValue(iName);
      updateMetadata(iMetadataName, iDescription, iType);
      hooks.put(iName, new ProfilerHookRuntime(iHookValue, iType));
    }
  }

  @Override
  public void unregisterHookValue(final String iName) {
    if (iName != null) {
      hooks.remove(iName);
    }
  }

  @Override
  public String getSystemMetric(final String iMetricName) {
    String buffer = "system." + iMetricName;
    return buffer;
  }

  @Override
  public String getProcessMetric(final String iMetricName) {
    String buffer = "process." + iMetricName;
    return buffer;
  }

  @Override
  public String getDatabaseMetric(final String iDatabaseName, final String iMetricName) {
    String buffer = "db." + (iDatabaseName != null ? iDatabaseName : "*") + '.' + iMetricName;
    return buffer;
  }

  @Override
  public String toJSON(String command, final String iPar1) {
    return null;
  }

  protected void installMemoryChecker() {
    final long memoryCheckInterval =
        GlobalConfiguration.PROFILER_MEMORYCHECK_INTERVAL.getValueAsLong();

    if (memoryCheckInterval > 0) {
      YouTrackDBManager.instance()
          .scheduleTask(new MemoryChecker(), memoryCheckInterval, memoryCheckInterval);
    }
  }

  /**
   * Updates the metric metadata.
   */
  protected void updateMetadata(
      final String iName, final String iDescription, final METRIC_TYPE iType) {
    if (iDescription != null && dictionary.putIfAbsent(iName, iDescription) == null) {
      types.put(iName, iType);
    }
  }

  @Override
  public void registerListener(ProfilerListener listener) {
    listeners.add(listener);
  }

  @Override
  public void unregisterListener(ProfilerListener listener) {
    listeners.remove(listener);
  }

  @Override
  public String threadDump() {
    final StringBuilder dump = new StringBuilder();
    dump.append("THREAD DUMP\n");
    final ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    final ThreadInfo[] threadInfos =
        threadMXBean.getThreadInfo(threadMXBean.getAllThreadIds(), 100);
    for (ThreadInfo threadInfo : threadInfos) {
      dump.append('"');
      dump.append(threadInfo.getThreadName());
      dump.append("\" ");
      final Thread.State state = threadInfo.getThreadState();
      dump.append("\n   java.lang.Thread.State: ");
      dump.append(state);
      final StackTraceElement[] stackTraceElements = threadInfo.getStackTrace();
      for (final StackTraceElement stackTraceElement : stackTraceElements) {
        dump.append("\n        at ");
        dump.append(stackTraceElement);
      }
      dump.append("\n\n");
    }
    return dump.toString();
  }

  @Override
  public METRIC_TYPE getType(final String k) {
    return types.get(k);
  }
}
