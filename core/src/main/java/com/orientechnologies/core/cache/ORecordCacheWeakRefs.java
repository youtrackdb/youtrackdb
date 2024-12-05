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

package com.orientechnologies.core.cache;

import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.YTRecordAbstract;
import com.orientechnologies.core.record.ORecordInternal;
import java.util.function.BiConsumer;

/**
 * Cache implementation that uses Soft References.
 */
public class ORecordCacheWeakRefs extends
    OAbstractMapCache<ORIDsWeakValuesHashMap<YTRecordAbstract>>
    implements ORecordCache {

  private static final BiConsumer<YTRID, YTRecordAbstract> UNLOAD_RECORDS_CONSUMER =
      (rid, record) -> {
        ORecordInternal.unsetDirty(record);
        record.unload();
      };

  private static final BiConsumer<YTRID, YTRecordAbstract> UNLOAD_NOT_MODIFIED_RECORDS_CONSUMER =
      (rid, record) -> {
        if (!record.isDirtyNoLoading()) {
          record.unload();
        }
      };

  public ORecordCacheWeakRefs() {
    super(new ORIDsWeakValuesHashMap<>());
  }

  @Override
  public YTRecordAbstract get(final YTRID rid) {
    if (!isEnabled()) {
      return null;
    }

    return cache.get(rid);
  }

  @Override
  public YTRecordAbstract put(final YTRecordAbstract record) {
    if (!isEnabled()) {
      return null;
    }
    return cache.put(record.getIdentity(), record);
  }

  @Override
  public YTRecordAbstract remove(final YTRID rid) {
    if (!isEnabled()) {
      return null;
    }
    return cache.remove(rid);
  }

  @Override
  public void unloadRecords() {
    cache.forEach(UNLOAD_RECORDS_CONSUMER);
  }

  @Override
  public void unloadNotModifiedRecords() {
    cache.forEach(UNLOAD_NOT_MODIFIED_RECORDS_CONSUMER);
  }

  @Override
  public void shutdown() {
    clear();
  }

  @Override
  public void clear() {
    cache.clear();
    cache = new ORIDsWeakValuesHashMap<>();
  }

  public void clearRecords() {
    for (YTRecord rec : cache.values()) {
      rec.clear();
    }
  }
}
