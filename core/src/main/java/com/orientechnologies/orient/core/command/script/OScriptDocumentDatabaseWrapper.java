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
package com.orientechnologies.orient.core.command.script;

import com.orientechnologies.common.util.OCommonConst;
import com.orientechnologies.orient.core.db.YTDatabaseSession;
import com.orientechnologies.orient.core.db.YTDatabaseSession.STATUS;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.record.YTIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.YTRID;
import com.orientechnologies.orient.core.id.YTRecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.YTSecurityUser;
import com.orientechnologies.orient.core.metadata.security.YTUser;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.impl.YTEntityImpl;
import com.orientechnologies.orient.core.sql.executor.YTResult;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import com.orientechnologies.orient.core.sql.query.OSQLQuery;
import com.orientechnologies.orient.core.tx.OTransaction;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Document Database wrapper class to use from scripts.
 */
@SuppressWarnings("unchecked")
@Deprecated
public class OScriptDocumentDatabaseWrapper {

  protected YTDatabaseSessionInternal database;

  public OScriptDocumentDatabaseWrapper(final YTDatabaseSessionInternal database) {
    this.database = database;
  }

  public YTIdentifiable[] query(final String iText) {
    return query(iText, (Object[]) null);
  }

  public YTIdentifiable[] query(final String iText, final Object... iParameters) {
    try (YTResultSet rs = database.query(iText, iParameters)) {
      return rs.stream().map(YTResult::toEntity).toArray(YTIdentifiable[]::new);
    }
  }

  public YTIdentifiable[] query(final OSQLQuery iQuery, final Object... iParameters) {
    final List<YTIdentifiable> res = database.query(iQuery, Arrays.asList(iParameters));
    if (res == null) {
      return OCommonConst.EMPTY_IDENTIFIABLE_ARRAY;
    }
    return res.toArray(new YTIdentifiable[0]);
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
    try (YTResultSet rs = database.command(iText, iParameters)) {
      return rs.stream().map(x -> x.toEntity()).toArray(size -> new YTIdentifiable[size]);
    }
  }

  public OIndex getIndex(final String name) {
    return database.getMetadata().getIndexManagerInternal().getIndex(database, name);
  }

  public boolean exists() {
    return database.exists();
  }

  public YTEntityImpl newInstance() {
    return database.newInstance();
  }

  public void reload() {
    database.reload();
  }

  public YTEntity newInstance(String iClassName) {
    return database.newInstance(iClassName);
  }

  public ORecordIteratorClass<YTEntityImpl> browseClass(String iClassName) {
    return database.browseClass(iClassName);
  }

  public STATUS getStatus() {
    return database.getStatus();
  }

  public ORecordIteratorClass<YTEntityImpl> browseClass(String iClassName, boolean iPolymorphic) {
    return database.browseClass(iClassName, iPolymorphic);
  }

  public YTDatabaseSession setStatus(STATUS iStatus) {
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

  public ORecordIteratorCluster<YTEntityImpl> browseCluster(String iClusterName) {
    return database.browseCluster(iClusterName);
  }

  public boolean isClosed() {
    return database.isClosed();
  }

  public YTDatabaseSession open(String iUserName, String iUserPassword) {
    return database.open(iUserName, iUserPassword);
  }

  public YTEntityImpl save(final Map<String, Object> iObject) {
    return database.save(new YTEntityImpl().fields(iObject));
  }

  public YTEntityImpl save(final String iString) {
    // return database.save((YTRecord) new YTEntityImpl().fromJSON(iString));
    return database.save(new YTEntityImpl().fromJSON(iString, true));
  }

  public YTEntityImpl save(YTRecord iRecord) {
    return database.save(iRecord);
  }

  public boolean dropCluster(String iClusterName) {
    return database.dropCluster(iClusterName);
  }

  public YTDatabaseSession create() {
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

  public OTransaction getTransaction() {
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

  public YTDatabaseSession setMVCC(boolean iValue) {
    return database.setMVCC(iValue);
  }

  public long getClusterRecordSizeById(int iClusterId) {
    return database.getClusterRecordSizeById(iClusterId);
  }

  public boolean isValidationEnabled() {
    return database.isValidationEnabled();
  }

  public long getClusterRecordSizeByName(String iClusterName) {
    return database.getClusterRecordSizeByName(iClusterName);
  }

  public YTDatabaseSession setValidationEnabled(boolean iValue) {
    return database.setValidationEnabled(iValue);
  }

  public YTSecurityUser getUser() {
    return database.getUser();
  }

  public void setUser(YTUser user) {
    database.setUser(user);
  }

  public OMetadata getMetadata() {
    return database.getMetadata();
  }

  public ODictionary<YTRecord> getDictionary() {
    return database.getDictionary();
  }

  public byte getRecordType() {
    return database.getRecordType();
  }

  public void delete(YTRID iRid) {
    database.delete(iRid);
  }

  public <RET extends YTRecord> RET load(YTRID iRecordId) {
    return database.load(iRecordId);
  }


  public int getDefaultClusterId() {
    return database.getDefaultClusterId();
  }

  public <RET extends YTRecord> RET load(final String iRidAsString) {
    return database.load(new YTRecordId(iRidAsString));
  }

  public YTDatabaseSession setDatabaseOwner(YTDatabaseSessionInternal iOwner) {
    return database.setDatabaseOwner(iOwner);
  }

  public Object setProperty(String iName, Object iValue) {
    return database.setProperty(iName, iValue);
  }

  public YTEntityImpl save(YTRecord iRecord, String iClusterName) {
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

  public YTDatabaseSession setRetainRecords(boolean iValue) {
    return database.setRetainRecords(iValue);
  }

  public long getSize() {
    return database.getSize();
  }

  public void delete(YTEntityImpl iRecord) {
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

  public String getType() {
    return database.getType();
  }
}
