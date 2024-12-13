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

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence.SEQUENCE_TYPE;
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
    return delegate.getSequenceNames(database);
  }

  @Override
  public int getSequenceCount() {
    return delegate.getSequenceCount(database);
  }

  @Override
  public Sequence getSequence(String iName) {
    return delegate.getSequence(database, iName);
  }

  @Override
  public Sequence createSequence(
      String iName, SEQUENCE_TYPE sequenceType, Sequence.CreateParams params)
      throws DatabaseException {
    boolean shouldGoOverDistributted =
        database.isDistributed() && (replicationProtocolVersion == 2);
    return createSequence(iName, sequenceType, params, shouldGoOverDistributted);
  }

  @Override
  Sequence createSequence(
      String iName,
      SEQUENCE_TYPE sequenceType,
      Sequence.CreateParams params,
      boolean executeViaDistributed)
      throws DatabaseException {
    if (executeViaDistributed) {
      SequenceAction action =
          new SequenceAction(SequenceAction.CREATE, iName, params, sequenceType);
      try {
        String sequenceName = database.sendSequenceAction(action);
        return delegate.getSequence(database, sequenceName);
      } catch (InterruptedException | ExecutionException exc) {
        LogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new DatabaseException(exc.getMessage());
      }
    } else {
      return delegate.createSequence(database, iName, sequenceType, params);
    }
  }

  @Override
  @Deprecated
  public void dropSequence(String iName) throws DatabaseException {
    boolean shouldGoOverDistributted =
        database.isDistributed() && (replicationProtocolVersion == 2);
    dropSequence(iName, shouldGoOverDistributted);
  }

  @Override
  void dropSequence(String iName, boolean executeViaDistributed) throws DatabaseException {
    if (executeViaDistributed) {
      SequenceAction action = new SequenceAction(SequenceAction.REMOVE, iName, null, null);
      try {
        database.sendSequenceAction(action);
      } catch (InterruptedException | ExecutionException exc) {
        LogManager.instance().error(this, exc.getMessage(), exc, (Object[]) null);
        throw new DatabaseException(exc.getMessage());
      }
    } else {
      delegate.dropSequence(database, iName);
    }
  }

  @Override
  public void create() {
    SequenceLibraryImpl.create(database);
  }

  @Override
  public void load() {
    delegate.load(database);
  }

  @Override
  public void close() {
    delegate.close();
  }

  public SequenceLibraryImpl getDelegate() {
    return delegate;
  }
}
