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

import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestTextAbstract;

/**
 * Execute a configured function.
 *
 * @see OCommandExecutorFunction
 */
public class OCommandFunction extends OCommandRequestTextAbstract {

  private static final long serialVersionUID = 1L;

  public OCommandFunction() {
  }

  public OCommandFunction(final String iName) {
    super(iName);
  }

  public boolean isIdempotent() {
    return false;
  }

  @Override
  public String toString() {
    return "function." + text;
  }
}
