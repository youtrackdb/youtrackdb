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
package com.jetbrains.youtrack.db.internal.core.command.script;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.DatabaseSession.ATTRIBUTES;
import com.jetbrains.youtrack.db.api.DatabaseSession.STATUS;
import com.jetbrains.youtrack.db.api.query.Result;
import com.jetbrains.youtrack.db.api.query.ResultSet;
import com.jetbrains.youtrack.db.api.record.Entity;
import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.api.record.RID;
import com.jetbrains.youtrack.db.api.record.Record;
import com.jetbrains.youtrack.db.api.security.SecurityUser;
import com.jetbrains.youtrack.db.internal.common.util.CommonConst;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.id.RecordId;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorClass;
import com.jetbrains.youtrack.db.internal.core.iterator.RecordIteratorCluster;
import com.jetbrains.youtrack.db.internal.core.metadata.Metadata;
import com.jetbrains.youtrack.db.internal.core.metadata.security.SecurityUserImpl;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import com.jetbrains.youtrack.db.internal.core.sql.query.SQLQuery;
import com.jetbrains.youtrack.db.internal.core.tx.FrontendTransaction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Document Database wrapper class to use from scripts.
 */
@Deprecated
public class ScriptDocumentDatabaseWrapper {

  protected DatabaseSessionInternal database;

  public ScriptDocumentDatabaseWrapper(final DatabaseSessionInternal database) {
    this.database = database;
  }

  public Map<?, ?>[] query(final String iText) {
    return query(iText, (Object[]) null);
  }

  public Map<?, ?>[] query(final String iText, final Object... iParameters) {
    try (ResultSet rs = database.query(iText, iParameters)) {
      return (Map<?, ?>[]) rs.stream().map(Result::toMap).toArray();
    }
  }

  public Identifiable[] query(final SQLQuery iQuery, final Object... iParameters) {
    final List<Identifiable> res = database.query(iQuery, Arrays.asList(iParameters));
    if (res == null) {
      return CommonConst.EMPTY_IDENTIFIABLE_ARRAY;
    }
    return res.toArray(new Identifiable[0]);
  }

  /**
   * To maintain the compatibility with JS API.
   */
  public Object executeCommand(final String iText) {
    return command(iText, (Object[]) null);
  }

  /**
   * To maintain the compatibility with JS API.
   */
  public Object executeCommand(final String iText, final Object... iParameters) {
    return command(iText, iParameters);
  }

  public Object command(final String iText) {
    return command(iText, (Object[]) null);
  }

  public Object command(final String iText, final Object... iParameters) {
    try (ResultSet rs = database.command(iText, iParameters)) {
      return rs.stream().map(Result::toMap).toArray();
    }
  }

  public Index getIndex(final String name) {
    return database.getMetadata().getIndexManagerInternal().getIndex(database, name);
  }

  public boolean exists() {
    return database.exists();
  }

  public EntityImpl newInstance() {
    return database.newInstance();
  }

  public void reload() {
    database.reload();
  }

  public Entity newInstance(String iClassName) {
    return database.newInstance(iClassName);
  }

  public RecordIteratorClass<EntityImpl> browseClass(String iClassName) {
    return database.browseClass(iClassName);
  }

  public STATUS getStatus() {
    return database.getStatus();
  }

  public RecordIteratorClass<EntityImpl> browseClass(String iClassName, boolean iPolymorphic) {
    return database.browseClass(iClassName, iPolymorphic);
  }

  public DatabaseSession setStatus(STATUS iStatus) {
    return database.setStatus(iStatus);
  }

  public void drop() {
    database.drop();
  }

  public String getName() {
    return database.getName();
  }

  public String getURL() {
    return database.getURL();
  }

  public RecordIteratorCluster<EntityImpl> browseCluster(String iClusterName) {
    return database.browseCluster(iClusterName);
  }

  public boolean isClosed() {
    return database.isClosed();
  }

  public DatabaseSession open(String iUserName, String iUserPassword) {
    return database.open(iUserName, iUserPassword);
  }

  public EntityImpl save(final Map<String, Object> iObject) {
    return database.save(new EntityImpl(database).fields(iObject));
  }

  public EntityImpl save(final String iString) {
    return database.save(new EntityImpl(database).updateFromJSON(iString, true));
  }

  public EntityImpl save(Record iRecord) {
    return database.save(iRecord);
  }

  public boolean dropCluster(String iClusterName) {
    return database.dropCluster(iClusterName);
  }

  public DatabaseSession create() {
    return database.create();
  }

  public boolean dropCluster(int iClusterId, final boolean iTruncate) {
    return database.dropCluster(iClusterId);
  }

  public void close() {
    database.close();
  }

  public int getClusters() {
    return database.getClusters();
  }

  public Collection<String> getClusterNames() {
    return database.getClusterNames();
  }

  public FrontendTransaction getTransaction() {
    return database.getTransaction();
  }

  public void begin() {
    database.begin();
  }

  public int getClusterIdByName(String iClusterName) {
    return database.getClusterIdByName(iClusterName);
  }

  public boolean isMVCC() {
    return database.isMVCC();
  }

  public String getClusterNameById(int iClusterId) {
    return database.getClusterNameById(iClusterId);
  }

  public DatabaseSession setMVCC(boolean iValue) {
    return database.setMVCC(iValue);
  }

  public boolean isValidationEnabled() {
    return database.isValidationEnabled();
  }

  public SecurityUser getUser() {
    return database.geCurrentUser();
  }

  public void setUser(SecurityUserImpl user) {
    database.setUser(user);
  }

  public Metadata getMetadata() {
    return database.getMetadata();
  }

  public byte getRecordType() {
    return database.getRecordType();
  }

  public void delete(RID iRid) {
    database.delete(iRid);
  }

  public <RET extends Record> RET load(RID iRecordId) {
    return database.load(iRecordId);
  }


  public int getDefaultClusterId() {
    return database.getDefaultClusterId();
  }

  public <RET extends Record> RET load(final String iRidAsString) {
    return database.load(new RecordId(iRidAsString));
  }

  public DatabaseSession setDatabaseOwner(DatabaseSessionInternal iOwner) {
    return database.setDatabaseOwner(iOwner);
  }

  public Object setProperty(String iName, Object iValue) {
    return database.setProperty(iName, iValue);
  }

  public EntityImpl save(Record iRecord, String iClusterName) {
    return database.save(iRecord, iClusterName);
  }

  public Object getProperty(String iName) {
    return database.getProperty(iName);
  }

  public Iterator<Entry<String, Object>> getProperties() {
    return database.getProperties();
  }

  public Object get(ATTRIBUTES iAttribute) {
    return database.get(iAttribute);
  }

  public void set(ATTRIBUTES attribute, Object iValue) {
    database.set(attribute, iValue);
  }

  public void setInternal(ATTRIBUTES attribute, Object iValue) {
    database.setInternal(attribute, iValue);
  }

  public boolean isRetainRecords() {
    return database.isRetainRecords();
  }

  public DatabaseSession setRetainRecords(boolean iValue) {
    return database.setRetainRecords(iValue);
  }

  public long getSize() {
    return database.getSize();
  }

  public void delete(EntityImpl iRecord) {
    database.delete(iRecord);
  }

  public long countClass(String iClassName) {
    return database.countClass(iClassName);
  }

  public void commit() {
    database.commit();
  }

  public void rollback() {
    database.rollback();
  }
}
