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
package com.orientechnologies.orient.core.serialization.serializer.record;

import com.orientechnologies.orient.core.OOrientListenerAbstract;
import com.orientechnologies.orient.core.Oxygen;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.ints.IntSet;

public class OSerializationThreadLocal extends ThreadLocal<IntSet> {

  public static volatile OSerializationThreadLocal INSTANCE = new OSerializationThreadLocal();

  static {
    Oxygen.instance()
        .registerListener(
            new OOrientListenerAbstract() {
              @Override
              public void onStartup() {
                if (INSTANCE == null) {
                  INSTANCE = new OSerializationThreadLocal();
                }
              }

              @Override
              public void onShutdown() {
                INSTANCE = null;
              }
            });
  }

  @Override
  protected IntSet initialValue() {
    return new IntOpenHashSet();
  }
}
