/*
 *
 *  *  Copyright YouTrackDB
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
package com.jetbrains.youtrack.db.internal.core.metadata.sequence;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence.SEQUENCE_TYPE;
import java.util.Set;
import java.util.concurrent.ExecutionException;

/**
 * @since 3/2/2015
 */
public class SequenceLibraryProxy extends SequenceLibraryAbstract {

  private static final int replicationProtocolVersion =
      GlobalConfiguration.DISTRIBUTED_REPLICATION_PROTOCOL_VERSION.getValue();

  public SequenceLibraryProxy(
      final SequenceLibraryImpl iDelegate, final DatabaseSessionInternal iDatabase) {
    super(iDelegate, iDatabase);
  }

  @Override
  public Set<String> getSequenceNames() {
    return delegate.getSequenceNames(session);
  }

  @Override
  public int getSequenceCount() {
    return delegate.getSequenceCount(session);
  }

  @Override
  public DBSequence getSequence(String iName) {
    return delegate.getSequence(session, iName);
  }

  @Override
  public DBSequence createSequence(
      String iName, SEQUENCE_TYPE sequenceType, DBSequence.CreateParams params)
      throws DatabaseException {
    var shouldGoOverDistributted =
        session.isDistributed() && (replicationProtocolVersion == 2);
    return createSequence(iName, sequenceType, params, shouldGoOverDistributted);
  }

  @Override
  DBSequence createSequence(
      String iName,
      SEQUENCE_TYPE sequenceType,
      DBSequence.CreateParams params,
      boolean executeViaDistributed)
      throws DatabaseException {
    if (executeViaDistributed) {
      var action =
          new SequenceAction(SequenceAction.CREATE, iName, params, sequenceType);
      try {
        String sequenceName = session.sendSequenceAction(action);
        return delegate.getSequence(session, sequenceName);
      } catch (InterruptedException | ExecutionException exc) {
        LogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new DatabaseException(session.getDatabaseName(), exc.getMessage());
      }
    } else {
      return delegate.createSequence(session, iName, sequenceType, params);
    }
  }

  @Override
  @Deprecated
  public void dropSequence(String iName) throws DatabaseException {
    var shouldGoOverDistributted =
        session.isDistributed() && (replicationProtocolVersion == 2);
    dropSequence(iName, shouldGoOverDistributted);
  }

  @Override
  void dropSequence(String iName, boolean executeViaDistributed) throws DatabaseException {
    if (executeViaDistributed) {
      var action = new SequenceAction(SequenceAction.REMOVE, iName, null, null);
      try {
        session.sendSequenceAction(action);
      } catch (InterruptedException | ExecutionException exc) {
        LogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new DatabaseException(session.getDatabaseName(), exc.getMessage());
      }
    } else {
      delegate.dropSequence(session, iName);
    }
  }

  @Override
  public void create() {
    SequenceLibraryImpl.create(session);
  }

  @Override
  public void load() {
    delegate.load(session);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public SequenceLibraryImpl getDelegate() {
    return delegate;
  }
}
