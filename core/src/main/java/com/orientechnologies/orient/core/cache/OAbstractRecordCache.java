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
package com.orientechnologies.orient.core.cache;

import com.orientechnologies.common.profiler.OAbstractProfiler.OProfilerHookValue;
import com.orientechnologies.common.profiler.OProfiler.METRIC_TYPE;
import com.orientechnologies.orient.core.YouTrackDBManager;
import com.orientechnologies.orient.core.id.YTRID;
import java.util.HashSet;
import java.util.Set;

/**
 * Cache of documents. Delegates real work on storing to {@link ORecordCache} implementation passed
 * at creation time leaving only DB specific functionality
 */
public abstract class OAbstractRecordCache {

  protected ORecordCache underlying;
  protected String profilerPrefix = "noname";
  protected String profilerMetadataPrefix = "noname";
  protected int excludedCluster = -1;

  /**
   * Create cache backed by given implementation
   *
   * @param impl actual implementation of cache
   */
  public OAbstractRecordCache(final ORecordCache impl) {
    underlying = impl;
  }

  /**
   * Tell whether cache is enabled
   *
   * @return {@code true} if cache enabled at call time, otherwise - {@code false}
   */
  public boolean isEnabled() {
    return underlying.isEnabled();
  }

  /**
   * Switch cache state between enabled and disabled
   *
   * @param enable pass {@code true} to enable, otherwise - {@code false}
   */
  public void setEnable(final boolean enable) {
    if (enable) {
      underlying.enable();
    } else {
      underlying.disable();
    }
  }

  /**
   * Remove all records belonging to specified cluster
   *
   * @param cid identifier of cluster
   */
  public void freeCluster(final int cid) {
    final Set<YTRID> toRemove = new HashSet<YTRID>(underlying.size() / 2);

    final Set<YTRID> keys = new HashSet<YTRID>(underlying.keys());
    for (final YTRID id : keys) {
      if (id.getClusterId() == cid) {
        toRemove.add(id);
      }
    }

    for (final YTRID ridToRemove : toRemove) {
      underlying.remove(ridToRemove);
    }
  }

  /**
   * Remove record entry
   *
   * @param rid unique record identifier
   */
  public void deleteRecord(final YTRID rid) {
    underlying.remove(rid);
  }

  /**
   * Clear the entire cache by removing all the entries
   */
  public void clear() {
    underlying.clear();
  }

  /**
   * Transfer all records contained in cache to unload state.
   */
  public void unloadRecords() {
    underlying.unloadRecords();
  }

  public void unloadNotModifiedRecords() {
    underlying.unloadNotModifiedRecords();
  }

  /**
   * Total number of cached entries
   *
   * @return non-negative integer
   */
  public int getSize() {
    return underlying.size();
  }

  /**
   * All operations running at cache initialization stage
   */
  public void startup() {
    underlying.startup();

    YouTrackDBManager.instance()
        .getProfiler()
        .registerHookValue(
            profilerPrefix + "current",
            "Number of entries in cache",
            METRIC_TYPE.SIZE,
            new OProfilerHookValue() {
              public Object getValue() {
                return getSize();
              }
            },
            profilerMetadataPrefix + "current");
  }

  /**
   * All operations running at cache destruction stage
   */
  public void shutdown() {
    underlying.shutdown();

    if (YouTrackDBManager.instance().getProfiler() != null) {
      YouTrackDBManager.instance().getProfiler().unregisterHookValue(profilerPrefix + "enabled");
      YouTrackDBManager.instance().getProfiler().unregisterHookValue(profilerPrefix + "current");
      YouTrackDBManager.instance().getProfiler().unregisterHookValue(profilerPrefix + "max");
    }
  }
}
