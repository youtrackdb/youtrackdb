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

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.encryption.impl.NothingEncryption;
import com.jetbrains.youtrack.db.internal.core.encryption.impl.AESEncryption;
import com.jetbrains.youtrack.db.internal.core.encryption.impl.AESGCMEncryption;
import com.jetbrains.youtrack.db.internal.core.encryption.impl.DESEncryption;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Factory of encryption algorithms.
 */
public class EncryptionFactory {

  public static final EncryptionFactory INSTANCE = new EncryptionFactory();

  private final Map<String, Encryption> instances = new HashMap<String, Encryption>();
  private final Map<String, Class<? extends Encryption>> classes =
      new HashMap<String, Class<? extends Encryption>>();

  /**
   * Install default encryption algorithms.
   */
  public EncryptionFactory() {
    register(NothingEncryption.class);
    register(DESEncryption.class);
    register(AESEncryption.class);
    register(AESGCMEncryption.class);
  }

  public Encryption getEncryption(final String name, final String iOptions) {
    Encryption encryption = instances.get(name);
    if (encryption == null) {

      final Class<? extends Encryption> encryptionClass;

      if (name == null) {
        encryptionClass = NothingEncryption.class;
      } else {
        encryptionClass = classes.get(name);
      }

      if (encryptionClass != null) {
        try {
          encryption = encryptionClass.newInstance();
          encryption.configure(iOptions);

        } catch (Exception e) {
          throw BaseException.wrapException(
              new SecurityException("Cannot instantiate encryption algorithm '" + name + "'"), e);
        }
      } else {
        throw new SecurityException("Encryption with name '" + name + "' is absent");
      }
    }
    return encryption;
  }

  /**
   * Registers a stateful implementations, a new instance will be created for each storage.
   *
   * @param iEncryption Encryption instance
   */
  public void register(final Encryption iEncryption) {
    try {
      final String name = iEncryption.name();

      if (instances.containsKey(name)) {
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");
      }

      if (classes.containsKey(name)) {
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");
      }

      instances.put(name, iEncryption);
    } catch (Exception e) {
      LogManager.instance()
          .error(this, "Cannot register storage encryption algorithm '%s'", e, iEncryption);
    }
  }

  /**
   * Registers a stateless implementations, the same instance will be shared on all the storages.
   *
   * @param iEncryption Encryption class
   */
  public void register(final Class<? extends Encryption> iEncryption) {
    try {
      final Encryption tempInstance = iEncryption.newInstance();

      final String name = tempInstance.name();

      if (instances.containsKey(name)) {
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");
      }

      if (classes.containsKey(tempInstance.name())) {
        throw new IllegalArgumentException(
            "Encryption with name '" + name + "' was already registered");
      }

      classes.put(name, iEncryption);
    } catch (Exception e) {
      LogManager.instance()
          .error(this, "Cannot register storage encryption algorithm '%s'", e, iEncryption);
    }
  }

  public Set<String> getInstances() {
    return instances.keySet();
  }
}
