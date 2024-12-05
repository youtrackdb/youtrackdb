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
package com.orientechnologies.core.metadata;

import com.orientechnologies.common.profiler.OProfiler;
import com.orientechnologies.core.YouTrackDBManager;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.OSharedContext;
import com.orientechnologies.core.index.OIndexManager;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.index.OIndexManagerProxy;
import com.orientechnologies.core.metadata.function.OFunctionLibrary;
import com.orientechnologies.core.metadata.function.OFunctionLibraryProxy;
import com.orientechnologies.core.metadata.schema.YTImmutableSchema;
import com.orientechnologies.core.metadata.schema.YTSchemaProxy;
import com.orientechnologies.core.metadata.security.OSecurity;
import com.orientechnologies.core.metadata.security.OSecurityProxy;
import com.orientechnologies.core.metadata.sequence.OSequenceLibrary;
import com.orientechnologies.core.metadata.sequence.OSequenceLibraryProxy;
import com.orientechnologies.core.schedule.OScheduler;
import com.orientechnologies.core.schedule.OSchedulerProxy;
import java.io.IOException;

public class OMetadataDefault implements OMetadataInternal {

  public static final String CLUSTER_INTERNAL_NAME = "internal";
  public static final String CLUSTER_INDEX_NAME = "index";
  public static final String CLUSTER_MANUAL_INDEX_NAME = "manindex";

  protected int schemaClusterId;

  protected YTSchemaProxy schema;
  protected OSecurity security;
  protected OIndexManagerProxy indexManager;
  protected OFunctionLibraryProxy functionLibrary;
  protected OSchedulerProxy scheduler;
  protected OSequenceLibraryProxy sequenceLibrary;

  protected static final OProfiler PROFILER = YouTrackDBManager.instance().getProfiler();

  private YTImmutableSchema immutableSchema = null;
  private int immutableCount = 0;
  private YTDatabaseSessionInternal database;

  public OMetadataDefault() {
  }

  public OMetadataDefault(YTDatabaseSessionInternal databaseDocument) {
    this.database = databaseDocument;
  }

  @Deprecated
  public void load() {
  }

  @Deprecated
  public void create() throws IOException {
  }

  public YTSchemaProxy getSchema() {
    return schema;
  }

  @Override
  public void makeThreadLocalSchemaSnapshot() {
    if (this.immutableCount == 0) {
      if (schema != null) {
        this.immutableSchema = schema.makeSnapshot();
      }
    }
    this.immutableCount++;
  }

  @Override
  public void clearThreadLocalSchemaSnapshot() {
    this.immutableCount--;
    if (this.immutableCount == 0) {
      this.immutableSchema = null;
    }
  }

  @Override
  public YTImmutableSchema getImmutableSchemaSnapshot() {
    if (immutableSchema == null) {
      if (schema == null) {
        return null;
      }
      return schema.makeSnapshot();
    }
    return immutableSchema;
  }

  public OSecurity getSecurity() {
    return security;
  }

  /**
   * {@inheritDoc}
   */
  @Deprecated
  public OIndexManager getIndexManager() {
    return indexManager;
  }

  @Override
  public OIndexManagerAbstract getIndexManagerInternal() {
    return indexManager.delegate();
  }

  public int getSchemaClusterId() {
    return schemaClusterId;
  }

  public OSharedContext init(OSharedContext shared) {
    schemaClusterId = database.getClusterIdByName(CLUSTER_INTERNAL_NAME);

    schema = new YTSchemaProxy(shared.getSchema(), database);
    indexManager = new OIndexManagerProxy(shared.getIndexManager(), database);
    security = new OSecurityProxy(shared.getSecurity(), database);
    functionLibrary = new OFunctionLibraryProxy(shared.getFunctionLibrary(), database);
    sequenceLibrary = new OSequenceLibraryProxy(shared.getSequenceLibrary(), database);
    scheduler = new OSchedulerProxy(shared.getScheduler(), database);
    return shared;
  }

  /**
   * Reloads the internal objects.
   */
  public void reload() {
    // RELOAD ALL THE SHARED CONTEXT
    database.getSharedContext().reload(database);
    // ADD HERE THE RELOAD OF A PROXY OBJECT IF NEEDED
  }

  /**
   * Closes internal objects
   */
  @Deprecated
  public void close() {
    // DO NOTHING BECAUSE THE PROXY OBJECT HAVE NO DIRECT STATE
    // ADD HERE THE CLOSE OF A PROXY OBJECT IF NEEDED
  }

  protected YTDatabaseSessionInternal getDatabase() {
    return database;
  }

  public OFunctionLibrary getFunctionLibrary() {
    return functionLibrary;
  }

  @Override
  public OSequenceLibrary getSequenceLibrary() {
    return sequenceLibrary;
  }

  public OScheduler getScheduler() {
    return scheduler;
  }
}
