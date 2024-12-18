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

import com.jetbrains.youtrack.db.internal.common.profiler.AbstractProfiler.ProfilerHookValue;
import com.jetbrains.youtrack.db.internal.common.util.Pair;
import com.jetbrains.youtrack.db.internal.common.util.Service;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.io.PrintStream;
import java.util.Date;
import java.util.List;
import java.util.Map;

public interface Profiler extends Service {

  enum METRIC_TYPE {
    CHRONO,
    COUNTER,
    STAT,
    SIZE,
    ENABLED,
    TEXT
  }

  METRIC_TYPE getType(String k);

  void updateCounter(String iStatName, String iDescription, long iPlus);

  void updateCounter(String iStatName, String iDescription, long iPlus, String iDictionary);

  long getCounter(String iStatName);

  String dump();

  String dump(String type);

  String dumpCounters();

  ProfilerEntry getChrono(String string);

  long startChrono();

  List<String> getChronos();

  long stopChrono(String iName, String iDescription, long iStartTime);

  long stopChrono(String iName, String iDescription, long iStartTime, String iDictionary);

  long stopChrono(
      String iName, String iDescription, long iStartTime, String iDictionary, String payload);

  long stopChrono(
      String iName,
      String iDescription,
      long iStartTime,
      String iDictionary,
      String payload,
      String user);

  String dumpChronos();

  String[] getCountersAsString();

  Date getLastReset();

  boolean isRecording();

  boolean startRecording();

  boolean stopRecording();

  void unregisterHookValue(String string);

  void configure(String string);

  void setAutoDump(int iNewValue);

  String metadataToJSON();

  Map<String, Pair<String, METRIC_TYPE>> getMetadata();

  String[] getHookAsString();

  Object getHookValue(String iName);

  void registerHookValue(
      String iName, String iDescription, METRIC_TYPE iType, ProfilerHookValue iHookValue);

  void registerHookValue(
      String iName,
      String iDescription,
      METRIC_TYPE iType,
      ProfilerHookValue iHookValue,
      String iMetadataName);

  String getSystemMetric(String iMetricName);

  String getProcessMetric(String iName);

  String getDatabaseMetric(String databaseName, String iName);

  String toJSON(String command, String iPar1);

  void resetRealtime(String iText);

  void dump(PrintStream out);

  int reportTip(String iMessage);

  void registerListener(ProfilerListener listener);

  void unregisterListener(ProfilerListener listener);

  String threadDump();

  String getStatsAsJson();

  default boolean isEnterpriseEdition() {
    return false;
  }

  default EntityImpl getContext() {
    return new EntityImpl(null).field("enterprise", false);
  }
}
