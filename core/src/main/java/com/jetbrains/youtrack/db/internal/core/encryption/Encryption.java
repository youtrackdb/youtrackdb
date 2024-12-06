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

package com.jetbrains.youtrack.db.internal.core.encryption;

/**
 * Storage encryption interface. Additional encryption implementations can be plugged via <code>
 * register()</code> method. There are 2 versions:<br>
 *
 * <ul>
 *   <li><code>EncryptionFactory.INSTANCE.register(<class>)</code> for stateful implementations, a
 *       new instance will be created for each storage/li>
 *   <li><code>EncryptionFactory.INSTANCE.register(<instance>)</code> for stateless
 *       implementations, the same instance will be shared across all the storages./li>
 * </ul>
 */
public interface Encryption {

  byte[] encrypt(byte[] content);

  byte[] decrypt(byte[] content);

  byte[] encrypt(byte[] content, final int offset, final int length);

  byte[] decrypt(byte[] content, final int offset, final int length);

  String name();

  Encryption configure(String iOptions);
}
