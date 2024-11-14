/*
 *
 *  *  Copyright 2010-2016 OrientDB LTD (http://orientdb.com)
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
 *  * For more information: http://orientdb.com
 *
 */
package com.orientechnologies.orient.core.db;

import com.orientechnologies.orient.core.db.document.ODatabaseDocument;
import com.orientechnologies.orient.core.db.record.OIdentifiable;
import com.orientechnologies.orient.core.exception.ODatabaseException;
import com.orientechnologies.orient.core.id.ORID;
import com.orientechnologies.orient.core.metadata.schema.OSchema;
import com.orientechnologies.orient.core.record.OEdge;
import com.orientechnologies.orient.core.record.OElement;
import com.orientechnologies.orient.core.record.OVertex;
import com.orientechnologies.orient.core.record.impl.OBlob;
import java.util.function.Supplier;
import javax.annotation.Nonnull;

/**
 * Session for database operations with a specific user.
 */
public interface ODatabaseSession extends ODatabaseDocument {

  /**
   * Returns the active session for the current thread.
   *
   * @return the active session for the current thread
   * @see #activateOnCurrentThread()
   * @see #isActiveOnCurrentThread()
   */
  static ODatabaseSession getActiveSession() {
    final ODatabaseRecordThreadLocal tl = ODatabaseRecordThreadLocal.instance();
    return tl.get();
  }

  /**
   * Executes the passed in code in a transaction. Starts a transaction if not already started, in
   * this case the transaction is committed after the code is executed or rolled back if an
   * exception is thrown.
   *
   * @param runnable Code to execute in transaction
   */
  void executeInTx(Runnable runnable);

  /**
   * Executes the given code in a transaction. Starts a transaction if not already started, in this
   * case the transaction is committed after the code is executed or rolled back if an exception is
   * thrown.
   *
   * @param supplier Code to execute in transaction
   * @param <T>      the type of the returned result
   * @return the result of the code execution
   */
  <T> T computeInTx(Supplier<T> supplier);

  /**
   * Binds current record to the session. It is mandatory to call this method in case you use
   * records that are not created or loaded by the session. Method returns bounded instance of given
   * record, usage of passed in instance is prohibited.
   *
   * @param identifiable Record to bind to the session, passed in instance is <b>prohibited</b> for
   *                     further usage.
   * @param <T>          Type of record.
   * @return Bounded instance of given record.
   */
  <T extends OIdentifiable> T bindToSession(T identifiable);

  /**
   * Returns the schema of the database.
   *
   * @return the schema of the database
   */
  default OSchema getSchema() {
    return getMetadata().getSchema();
  }

  /**
   * Returns the number of active nested transactions.
   *
   * @return the number of active transactions, 0 means no active transactions are present.
   * @see #begin()
   * @see #commit()
   * @see #rollback()
   */
  int activeTxCount();

  /**
   * Loads an element by its id, throws an exception if record is not an element or does not exist.
   *
   * @param id the id of the element to load
   * @return the loaded element
   * @throws ODatabaseException                                                   if the record is
   *                                                                              not an element
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default OElement loadElement(ORID id) {
    var record = load(id);
    if (record instanceof OElement element) {
      return element;
    }

    throw new ODatabaseException(
        "Record with id " + id + " is not an element, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads a vertex by its id, throws an exception if record is not a vertex or does not exist.
   *
   * @param id the id of the vertex to load
   * @return the loaded vertex
   * @throws ODatabaseException                                                   if the record is
   *                                                                              not a vertex
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default OVertex loadVertex(ORID id) {
    var record = load(id);
    if (record instanceof OVertex vertex) {
      return vertex;
    }

    throw new ODatabaseException(
        "Record with id " + id + " is not a vertex, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads an edge by its id, throws an exception if record is not an edge or does not exist.
   *
   * @param id the id of the edge to load
   * @return the loaded edge
   * @throws ODatabaseException                                                   if the record is
   *                                                                              not an edge
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default OEdge loadEdge(ORID id) {
    var record = load(id);
    if (record instanceof OEdge edge) {
      return edge;
    }

    throw new ODatabaseException(
        "Record with id " + id + " is not an edge, but a " + record.getClass().getSimpleName());
  }

  /**
   * Loads a blob by its id, throws an exception if record is not a blob or does not exist.
   *
   * @param id the id of the blob to load
   * @return the loaded blob
   * @throws ODatabaseException                                                   if the record is
   *                                                                              not a blob
   * @throws com.orientechnologies.orient.core.exception.ORecordNotFoundException if the record does
   *                                                                              not exist
   */
  @Nonnull
  default OBlob loadBlob(ORID id) {
    var record = load(id);
    if (record instanceof OBlob blob) {
      return blob;
    }

    throw new ODatabaseException(
        "Record with id " + id + " is not a blob, but a " + record.getClass().getSimpleName());
  }
}
