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

import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.record.Edge;
import com.jetbrains.youtrack.db.internal.core.record.Entity;
import com.jetbrains.youtrack.db.internal.core.record.Record;
import com.jetbrains.youtrack.db.internal.core.record.Vertex;
import com.jetbrains.youtrack.db.internal.core.record.impl.Blob;
import com.jetbrains.youtrack.db.internal.core.sql.executor.YTResultSet;
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

  public Entity newInstance() {
    return this.database.newInstance();
  }

  public Entity newInstance(String className) {
    return this.database.newInstance(className);
  }

  public Vertex newVertex() {
    return this.database.newVertex();
  }

  public Vertex newVertex(String className) {
    return this.database.newVertex(className);
  }

  public Edge newEdge(Vertex from, Vertex to) {
    return this.database.newEdge(from, to);
  }

  public Edge newEdge(Vertex from, Vertex to, String edgeClassName) {
    return this.database.newEdge(from, to, edgeClassName);
  }

  public Record save(Record element) {
    return this.database.save(element);
  }

  public void delete(Record record) {
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

  public Blob newBlob() {
    return this.database.newBlob();
  }
}
