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

import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.record.YTEdge;
import com.orientechnologies.orient.core.record.YTEntity;
import com.orientechnologies.orient.core.record.YTRecord;
import com.orientechnologies.orient.core.record.YTVertex;
import com.orientechnologies.orient.core.record.impl.YTBlob;
import com.orientechnologies.orient.core.sql.executor.YTResultSet;
import java.util.Map;

/**
 * Database wrapper class to use from scripts.
 */
public class OScriptDatabaseWrapper {

  protected YTDatabaseSessionInternal database;

  public OScriptDatabaseWrapper(final YTDatabaseSessionInternal database) {
    this.database = database;
  }

  public YTResultSet query(final String iText, final Object... iParameters) {
    return this.database.query(iText, iParameters);
  }

  public YTResultSet query(final String query, Map<String, Object> iParameters) {
    return this.database.query(query, iParameters);
  }

  public YTResultSet command(final String iText, final Object... iParameters) {
    return this.database.command(iText, iParameters);
  }

  public YTResultSet command(final String query, Map<String, Object> iParameters) {
    return this.database.query(query, iParameters);
  }

  public YTResultSet execute(String language, final String script, final Object... iParameters) {
    return this.database.execute(language, script, iParameters);
  }

  public YTResultSet execute(String language, final String script,
      Map<String, Object> iParameters) {
    return this.database.execute(language, script, iParameters);
  }

  public YTEntity newInstance() {
    return this.database.newInstance();
  }

  public YTEntity newInstance(String className) {
    return this.database.newInstance(className);
  }

  public YTVertex newVertex() {
    return this.database.newVertex();
  }

  public YTVertex newVertex(String className) {
    return this.database.newVertex(className);
  }

  public YTEdge newEdge(YTVertex from, YTVertex to) {
    return this.database.newEdge(from, to);
  }

  public YTEdge newEdge(YTVertex from, YTVertex to, String edgeClassName) {
    return this.database.newEdge(from, to, edgeClassName);
  }

  public YTRecord save(YTRecord element) {
    return this.database.save(element);
  }

  public void delete(YTRecord record) {
    this.database.delete(record);
  }

  public void commit() {
    database.commit();
  }

  public void rollback() {
    database.rollback();
  }

  public void begin() {
    database.begin();
  }

  public YTBlob newBlob() {
    return this.database.newBlob();
  }
}
