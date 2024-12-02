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
import com.orientechnologies.orient.core.db.ODatabaseSession;
import com.orientechnologies.orient.core.db.ODatabaseSession.STATUS;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal;
import com.orientechnologies.orient.core.db.ODatabaseSessionInternal.ATTRIBUTES;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.dictionary.ODictionary;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.id.ORecordId;
import com.orientechnologies.orient.core.index.OIndex;
import com.orientechnologies.orient.core.iterator.ORecordIteratorClass;
import com.orientechnologies.orient.core.iterator.ORecordIteratorCluster;
import com.orientechnologies.orient.core.metadata.OMetadata;
import com.orientechnologies.orient.core.metadata.security.OSecurityUser;
import com.orientechnologies.orient.core.metadata.security.OUser;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.ORecord;
import com.orientechnologies.orient.core.record.impl.ODocument;
import com.orientechnologies.orient.core.sql.executor.OResult;
import com.orientechnologies.orient.core.sql.executor.OResultSet;
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

  protected ODatabaseSessionInternal database;

  public OScriptDocumentDatabaseWrapper(final ODatabaseSessionInternal database) {
    this.database = database;
  }

  public OIdentifiable[] query(final String iText) {
    return query(iText, (Object[]) null);
  }

  public OIdentifiable[] query(final String iText, final Object... iParameters) {
    try (OResultSet rs = database.query(iText, iParameters)) {
      return rs.stream().map(OResult::toElement).toArray(OIdentifiable[]::new);
    }
  }

  public OIdentifiable[] query(final OSQLQuery iQuery, final Object... iParameters) {
    final List<OIdentifiable> res = database.query(iQuery, Arrays.asList(iParameters));
    if (res == null) {
      return OCommonConst.EMPTY_IDENTIFIABLE_ARRAY;
    }
    return res.toArray(new OIdentifiable[0]);
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
    try (OResultSet rs = database.command(iText, iParameters)) {
      return rs.stream().map(x -> x.toElement()).toArray(size -> new OIdentifiable[size]);
    }
  }

  public OIndex getIndex(final String name) {
    return database.getMetadata().getIndexManagerInternal().getIndex(database, name);
  }

  public boolean exists() {
    return database.exists();
  }

  public ODocument newInstance() {
    return database.newInstance();
  }

  public void reload() {
    database.reload();
  }

  public OElement newInstance(String iClassName) {
    return database.newInstance(iClassName);
  }

  public ORecordIteratorClass<ODocument> browseClass(String iClassName) {
    return database.browseClass(iClassName);
  }

  public STATUS getStatus() {
    return database.getStatus();
  }

  public ORecordIteratorClass<ODocument> browseClass(String iClassName, boolean iPolymorphic) {
    return database.browseClass(iClassName, iPolymorphic);
  }

  public ODatabaseSession setStatus(STATUS iStatus) {
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

  public ORecordIteratorCluster<ODocument> browseCluster(String iClusterName) {
    return database.browseCluster(iClusterName);
  }

  public boolean isClosed() {
    return database.isClosed();
  }

  public ODatabaseSession open(String iUserName, String iUserPassword) {
    return database.open(iUserName, iUserPassword);
  }

  public ODocument save(final Map<String, Object> iObject) {
    return database.save(new ODocument().fields(iObject));
  }

  public ODocument save(final String iString) {
    // return database.save((ORecord) new ODocument().fromJSON(iString));
    return database.save(new ODocument().fromJSON(iString, true));
  }

  public ODocument save(ORecord iRecord) {
    return database.save(iRecord);
  }

  public boolean dropCluster(String iClusterName) {
    return database.dropCluster(iClusterName);
  }

  public ODatabaseSession create() {
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

  public ODatabaseSession setMVCC(boolean iValue) {
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

  public ODatabaseSession setValidationEnabled(boolean iValue) {
    return database.setValidationEnabled(iValue);
  }

  public OSecurityUser getUser() {
    return database.getUser();
  }

  public void setUser(OUser user) {
    database.setUser(user);
  }

  public OMetadata getMetadata() {
    return database.getMetadata();
  }

  public ODictionary<ORecord> getDictionary() {
    return database.getDictionary();
  }

  public byte getRecordType() {
    return database.getRecordType();
  }

  public void delete(ORID iRid) {
    database.delete(iRid);
  }

  public <RET extends ORecord> RET load(ORID iRecordId) {
    return database.load(iRecordId);
  }

  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan) {
    return database.load(iRecordId, iFetchPlan);
  }

  public <RET extends ORecord> RET load(ORID iRecordId, String iFetchPlan, boolean iIgnoreCache) {
    return database.load(iRecordId, iFetchPlan, iIgnoreCache);
  }

  public int getDefaultClusterId() {
    return database.getDefaultClusterId();
  }

  public <RET extends ORecord> RET load(final String iRidAsString) {
    return database.load(new ORecordId(iRidAsString));
  }

  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan) {
    return database.load(iRecord, iFetchPlan);
  }

  public <RET extends ORecord> RET load(ORecord iRecord, String iFetchPlan, boolean iIgnoreCache) {
    return database.load(iRecord, iFetchPlan, iIgnoreCache);
  }

  public ODatabaseSession setDatabaseOwner(ODatabaseSessionInternal iOwner) {
    return database.setDatabaseOwner(iOwner);
  }

  public Object setProperty(String iName, Object iValue) {
    return database.setProperty(iName, iValue);
  }

  public ODocument save(ORecord iRecord, String iClusterName) {
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

  public ODatabaseSession setRetainRecords(boolean iValue) {
    return database.setRetainRecords(iValue);
  }

  public long getSize() {
    return database.getSize();
  }

  public void delete(ODocument iRecord) {
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
