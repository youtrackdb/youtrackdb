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
package com.orientechnologies.core.db.record;

import com.orientechnologies.core.db.YTDatabaseSessionInternal;

/**
 * Generic proxy abstratc class.
 */
public abstract class OProxedResource<T> {

  protected final T delegate;
  protected final YTDatabaseSessionInternal database;

  protected OProxedResource(final T iDelegate, final YTDatabaseSessionInternal iDatabase) {
    this.delegate = iDelegate;
    this.database = iDatabase;
  }
}
