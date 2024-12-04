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
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.YTDatabaseSessionEmbedded;
import com.orientechnologies.orient.core.storage.OStorage;

/**
 *
 */
public class YTDatabaseSessionEmbeddedPooled extends YTDatabaseSessionEmbedded {

  private final ODatabasePoolInternal pool;

  public YTDatabaseSessionEmbeddedPooled(ODatabasePoolInternal pool, OStorage storage) {
    super(storage);
    this.pool = pool;
  }

  @Override
  public void close() {
    if (isClosed()) {
      return;
    }
    internalClose(true);
    pool.release(this);
  }

  public void reuse() {
    activateOnCurrentThread();
    setStatus(STATUS.OPEN);
  }

  @Override
  public YTDatabaseSessionInternal copy() {
    return (YTDatabaseSessionInternal) pool.acquire();
  }

  public void realClose() {
    YTDatabaseSessionInternal old = ODatabaseRecordThreadLocal.instance().getIfDefined();
    try {
      activateOnCurrentThread();
      super.close();
    } finally {
      if (old == null) {
        ODatabaseRecordThreadLocal.instance().remove();
      } else {
        ODatabaseRecordThreadLocal.instance().set(old);
      }
    }
  }
}
