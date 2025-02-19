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
package com.jetbrains.youtrack.db.internal.core.command.traverse;

import com.jetbrains.youtrack.db.api.record.Identifiable;
import com.jetbrains.youtrack.db.internal.core.command.CommandProcess;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;

public abstract class TraverseAbstractProcess<T>
    extends CommandProcess<Traverse, T, Identifiable> {

  protected final DatabaseSessionInternal session;


  public TraverseAbstractProcess(final Traverse iCommand, final T iTarget,
      DatabaseSessionInternal session) {
    super(iCommand, iTarget);
    this.session = session;
  }

  public Identifiable pop() {
    command.getContext().pop(null);
    return null;
  }

  public abstract TraversePath getPath();
}
