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
import java.util.function.Supplier;

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
   *               further usage.
   * @param <T>    Type of record.
   * @return Bounded instance of given record.
   */
  <T extends OIdentifiable> T bindToSession(T identifiable);
}
