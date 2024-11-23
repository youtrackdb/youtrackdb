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

import com.orientechnologies.common.concur.lock.OAdaptiveLock;
import com.orientechnologies.common.concur.lock.OLockException;
import com.orientechnologies.common.concur.resource.OReentrantResourcePool;
import com.orientechnologies.common.concur.resource.OResourcePoolListener;
import com.orientechnologies.common.io.OIOUtils;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.OOrientListener;
import com.orientechnologies.orient.core.Oxygen;
import com.orientechnologies.orient.core.config.OGlobalConfiguration;
import com.orientechnologies.orient.core.storage.OStorage;
import it.unimi.dsi.fastutil.objects.Object2LongMap;
import it.unimi.dsi.fastutil.objects.Object2LongOpenHashMap;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;

public abstract class ODatabasePoolAbstract extends OAdaptiveLock
    implements OResourcePoolListener<String, ODatabaseSession>, OOrientListener {

  private final HashMap<String, OReentrantResourcePool<String, ODatabaseSession>> pools =
      new HashMap<>();
  protected Object owner;
  private final int maxSize;
  private final int timeout;
  private volatile Timer evictionTask;
  private Evictor evictor;

  /**
   * The idle object evictor {@link TimerTask}.
   */
  class Evictor extends TimerTask {

    private final HashMap<String, Object2LongOpenHashMap<ODatabaseSessionInternal>> evictionMap =
        new HashMap<>();
    private final long minIdleTime;

    public Evictor(long minIdleTime) {
      this.minIdleTime = minIdleTime;
    }

    /**
     * Run pool maintenance. Evict objects qualifying for eviction
     */
    @Override
    public void run() {
      OLogManager.instance().debug(this, "Running Connection Pool Evictor Service...");
      lock();
      try {
        for (Entry<String, Object2LongOpenHashMap<ODatabaseSessionInternal>> pool :
            this.evictionMap.entrySet()) {
          Object2LongOpenHashMap<ODatabaseSessionInternal> poolDbs = pool.getValue();
          Iterator<Object2LongMap.Entry<ODatabaseSessionInternal>> iterator =
              poolDbs.object2LongEntrySet().iterator();
          while (iterator.hasNext()) {
            Entry<ODatabaseSessionInternal, Long> db = iterator.next();
            if (System.currentTimeMillis() - db.getValue() >= this.minIdleTime) {

              OReentrantResourcePool<String, ODatabaseSession> oResourcePool =
                  pools.get(pool.getKey());
              if (oResourcePool != null) {
                OLogManager.instance()
                    .debug(this, "Closing idle pooled database '%s'...", db.getKey().getName());
                ((ODatabasePooled) db.getKey()).forceClose();
                oResourcePool.remove(db.getKey());
                iterator.remove();
              }
            }
          }
        }
      } finally {
        unlock();
      }
    }

    public void updateIdleTime(final String poolName, final ODatabaseSessionInternal iDatabase) {
      Object2LongOpenHashMap<ODatabaseSessionInternal> pool = this.evictionMap.get(poolName);
      if (pool == null) {
        pool = new Object2LongOpenHashMap<>();
        pool.defaultReturnValue(-1);

        this.evictionMap.put(poolName, pool);
      }

      pool.put(iDatabase, System.currentTimeMillis());
    }
  }

  public ODatabasePoolAbstract(
      final Object iOwner,
      final int iMinSize,
      final int iMaxSize,
      final long idleTimeout,
      final long timeBetweenEvictionRunsMillis) {
    this(
        iOwner,
        iMinSize,
        iMaxSize,
        OGlobalConfiguration.CLIENT_CONNECT_POOL_WAIT_TIMEOUT.getValueAsInteger(),
        idleTimeout,
        timeBetweenEvictionRunsMillis);
  }

  public ODatabasePoolAbstract(
      final Object iOwner,
      final int iMinSize,
      final int iMaxSize,
      final int iTimeout,
      final long idleTimeoutMillis,
      final long timeBetweenEvictionRunsMillis) {
    super(true, OGlobalConfiguration.STORAGE_LOCK_TIMEOUT.getValueAsInteger(), true);

    maxSize = iMaxSize;
    timeout = iTimeout;
    owner = iOwner;
    Oxygen.instance().registerListener(this);

    if (idleTimeoutMillis > 0 && timeBetweenEvictionRunsMillis > 0) {
      this.evictionTask = new Timer();
      this.evictor = new Evictor(idleTimeoutMillis);
      this.evictionTask.schedule(
          evictor, timeBetweenEvictionRunsMillis, timeBetweenEvictionRunsMillis);
    }
  }

  public ODatabaseSession acquire(
      final String iURL, final String iUserName, final String iUserPassword) throws OLockException {
    return acquire(iURL, iUserName, iUserPassword, null);
  }

  public ODatabaseSession acquire(
      final String iURL,
      final String iUserName,
      final String iUserPassword,
      final Map<String, Object> iOptionalParams)
      throws OLockException {
    final String dbPooledName = OIOUtils.getUnixFileName(iUserName + "@" + iURL);
    OReentrantResourcePool<String, ODatabaseSession> pool;
    lock();
    try {
      pool = pools.get(dbPooledName);
      if (pool == null)
      // CREATE A NEW ONE
      {
        pool = new OReentrantResourcePool<String, ODatabaseSession>(maxSize, this);
      }

      // PUT IN THE POOL MAP ONLY IF AUTHENTICATION SUCCEED
      pools.put(dbPooledName, pool);

    } finally {
      unlock();
    }
    return pool.getResource(iURL, timeout, iUserName, iUserPassword, iOptionalParams);
  }

  public int getMaxConnections(final String url, final String userName) {
    final String dbPooledName = OIOUtils.getUnixFileName(userName + "@" + url);
    final OReentrantResourcePool<String, ODatabaseSession> pool;
    lock();
    try {
      pool = pools.get(dbPooledName);
    } finally {
      unlock();
    }
    if (pool == null) {
      return maxSize;
    }

    return pool.getMaxResources();
  }

  public int getCreatedInstances(String url, String userName) {
    final String dbPooledName = OIOUtils.getUnixFileName(userName + "@" + url);
    lock();
    try {
      final OReentrantResourcePool<String, ODatabaseSession> pool = pools.get(dbPooledName);
      if (pool == null) {
        return 0;
      }

      return pool.getCreatedInstances();
    } finally {
      unlock();
    }
  }

  public int getAvailableConnections(final String url, final String userName) {
    final String dbPooledName = OIOUtils.getUnixFileName(userName + "@" + url);
    final OReentrantResourcePool<String, ODatabaseSession> pool;
    lock();
    try {
      pool = pools.get(dbPooledName);
    } finally {
      unlock();
    }
    if (pool == null) {
      return 0;
    }

    return pool.getAvailableResources();
  }

  public int getConnectionsInCurrentThread(final String url, final String userName) {
    final String dbPooledName = OIOUtils.getUnixFileName(userName + "@" + url);
    final OReentrantResourcePool<String, ODatabaseSession> pool;
    lock();
    try {
      pool = pools.get(dbPooledName);
    } finally {
      unlock();
    }
    if (pool == null) {
      return 0;
    }

    return pool.getConnectionsInCurrentThread(url);
  }

  public void release(final ODatabaseSessionInternal iDatabase) {
    final String dbPooledName = iDatabase.getUser().getName(iDatabase) + "@" + iDatabase.getURL();
    final OReentrantResourcePool<String, ODatabaseSession> pool;
    lock();
    try {

      pool = pools.get(dbPooledName);

    } finally {
      unlock();
    }
    if (pool == null) {
      throw new OLockException(
          "Cannot release a database URL not acquired before. URL: " + iDatabase.getName());
    }

    if (pool.returnResource(iDatabase)) {
      this.notifyEvictor(dbPooledName, iDatabase);
    }
  }

  public Map<String, OReentrantResourcePool<String, ODatabaseSession>> getPools() {
    lock();
    try {

      return Collections.unmodifiableMap(pools);

    } finally {
      unlock();
    }
  }

  /**
   * Closes all the databases.
   */
  public void close() {
    lock();
    try {

      if (this.evictionTask != null) {
        this.evictionTask.cancel();
      }

      for (Entry<String, OReentrantResourcePool<String, ODatabaseSession>> pool :
          pools.entrySet()) {
        for (ODatabaseSession db : pool.getValue().getResources()) {
          pool.getValue().close();
          try {
            OLogManager.instance().debug(this, "Closing pooled database '%s'...", db.getName());
            ((ODatabasePooled) db).forceClose();
            OLogManager.instance().debug(this, "OK", db.getName());
          } catch (Exception e) {
            OLogManager.instance().debug(this, "Error: %d", e.toString());
          }
        }
      }

    } finally {
      unlock();
    }
  }

  public void remove(final String iName, final String iUser) {
    remove(iUser + "@" + iName);
  }

  public void remove(final String iPoolName) {
    lock();
    try {

      final OReentrantResourcePool<String, ODatabaseSession> pool = pools.remove(iPoolName);

      if (pool != null) {
        for (ODatabaseSession db : pool.getResources()) {
          final OStorage stg = ((ODatabaseSessionInternal) db).getStorage();
          if (stg != null && stg.getStatus() == OStorage.STATUS.OPEN) {
            try {
              OLogManager.instance().debug(this, "Closing pooled database '%s'...", db.getName());
              db.activateOnCurrentThread();
              ((ODatabasePooled) db).forceClose();
              OLogManager.instance().debug(this, "OK", db.getName());
            } catch (Exception e) {
              OLogManager.instance().debug(this, "Error: %d", e.toString());
            }
          }
        }
        pool.close();
      }

    } finally {
      unlock();
    }
  }

  public int getMaxSize() {
    return maxSize;
  }

  public void onStorageRegistered(final OStorage iStorage) {
  }

  /**
   * Removes from memory the pool associated to the closed storage. This avoids pool open against
   * closed storages.
   */
  public void onStorageUnregistered(final OStorage iStorage) {
    final String storageURL = iStorage.getURL();

    lock();
    try {
      Set<String> poolToClose = null;

      for (Entry<String, OReentrantResourcePool<String, ODatabaseSession>> e : pools.entrySet()) {
        final int pos = e.getKey().indexOf('@');
        final String dbName = e.getKey().substring(pos + 1);
        if (storageURL.equals(dbName)) {
          if (poolToClose == null) {
            poolToClose = new HashSet<>();
          }

          poolToClose.add(e.getKey());
        }
      }

      if (poolToClose != null) {
        for (String pool : poolToClose) {
          remove(pool);
        }
      }

    } finally {
      unlock();
    }
  }

  @Override
  public void onShutdown() {
    close();
  }

  private void notifyEvictor(final String poolName, final ODatabaseSessionInternal iDatabase) {
    if (this.evictor != null) {
      this.evictor.updateIdleTime(poolName, iDatabase);
    }
  }
}
