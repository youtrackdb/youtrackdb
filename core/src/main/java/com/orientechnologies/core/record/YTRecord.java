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
package com.orientechnologies.core.record;

import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.tx.OTransactionOptimistic;
import java.io.Serializable;

/**
 * Generic record representation.
 */
public interface YTRecord extends YTIdentifiable, Serializable {

  /**
   * Returns true if the record is unloaded.
   *
   * @return true if the record is unloaded.
   */
  boolean isUnloaded();

  /**
   * Returns <code>true</code> if record is bound to the passed in session.
   *
   * @param session The session to check
   * @return <code>true</code> if record is bound to the passed in session.
   * @see YTDatabaseSession#bindToSession(YTIdentifiable)
   */
  boolean isNotBound(YTDatabaseSession session);

  /**
   * All the fields are deleted but the record identity is maintained. Use this to remove all the
   * document's fields.
   */
  void clear();

  /**
   * Returns the record identity as &lt;cluster-id&gt;:&lt;cluster-position&gt;
   */
  YTRID getIdentity();

  /**
   * Returns the current version number of the record. When the record is created has version = 0.
   * At every change the storage increment the version number. Version number is used by Optimistic
   * transactions to check if the record is changed in the meanwhile of the transaction.
   *
   * @return The version number. 0 if it's a brand new record.
   * @see OTransactionOptimistic
   */
  int getVersion();

  /**
   * Checks if the record is dirty, namely if it was changed in memory.
   *
   * @return True if dirty, otherwise false
   */
  boolean isDirty();

  /**
   * Saves in-memory changes to the database. Behavior depends by the current running transaction if
   * any. If no transaction is running then changes apply immediately. If an Optimistic transaction
   * is running then the record will be changed at commit time. The current transaction will
   * continue to see the record as modified, while others not. If a Pessimistic transaction is
   * running, then an exclusive lock is acquired against the record. Current transaction will
   * continue to see the record as modified, while others cannot access to it since it's locked.
   */
  void save();

  /**
   * Deletes the record from the database. Behavior depends by the current running transaction if
   * any. If no transaction is running then the record is deleted immediately. If an Optimistic
   * transaction is running then the record will be deleted at commit time. The current transaction
   * will continue to see the record as deleted, while others not. If a Pessimistic transaction is
   * running, then an exclusive lock is acquired against the record. Current transaction will
   * continue to see the record as deleted, while others cannot access to it since it's locked.
   */
  void delete();

  /**
   * Fills the record parsing the content in JSON format.
   *
   * @param iJson Object content in JSON format
   */
  void fromJSON(String iJson);

  /**
   * Exports the record in JSON format.
   *
   * @return Object content in JSON format
   */
  String toJSON();

  /**
   * Exports the record in JSON format specifying additional formatting settings.
   *
   * @param iFormat Format settings separated by comma. Available settings are:
   *                <ul>
   *                  <li><b>rid</b>: exports the record's id as property "@rid"
   *                  <li><b>version</b>: exports the record's version as property "@version"
   *                  <li><b>class</b>: exports the record's class as property "@class"
   *                  <li><b>attribSameRow</b>: exports all the record attributes in the same row
   *                  <li><b>indent:&lt;level&gt;</b>: Indents the output if the &lt;level&gt; specified.
   *                      Default is 0
   *                </ul>
   *                Example: "rid,version,class,indent:6" exports record id, version and class properties along
   *                with record properties using an indenting level equals of 6.
   * @return Object content in JSON format
   */
  String toJSON(String iFormat);

  /**
   * Checks if the record exists in the database. It adheres the same rules
   * {@link YTDatabaseSession#exists(YTRID)}.
   *
   * @return true if the record exists, otherwise false
   */
  boolean exists();
}
