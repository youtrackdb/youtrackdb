/*
 *
 *  *  Copyright 2016 YouTrackDB LTD
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
package com.orientechnologies.orient.core.security.symmetrickey;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.orient.core.config.YTGlobalConfiguration;
import com.orientechnologies.orient.core.exception.YTSecurityException;
import com.orientechnologies.orient.core.record.impl.YTDocument;
import com.orientechnologies.orient.core.security.OCredentialInterceptor;

/**
 * Provides a symmetric key credential interceptor.
 *
 * <p>The "password" parameter should be a JSON document specifying "keyAlgorithm" and "key",
 * "keyFile", or "keyStore".
 *
 * <p>The method getPassword() will return a Base64-encoded JSON document with the encrypted
 * "username" as its payload.
 */
public class OSymmetricKeyCI implements OCredentialInterceptor {

  private String username;
  private String encodedJSON = "";

  public String getUsername() {
    return this.username;
  }

  public String getPassword() {
    return this.encodedJSON;
  }

  /**
   * The usual password field should be a JSON representation.
   */
  public void intercept(final String url, final String username, final String password)
      throws YTSecurityException {
    if (username == null || username.isEmpty()) {
      throw new YTSecurityException("OSymmetricKeyCI username is not valid!");
    }
    if (password == null || password.isEmpty()) {
      throw new YTSecurityException("OSymmetricKeyCI password is not valid!");
    }

    this.username = username;

    // These are all used as defaults if the JSON document is missing any fields.

    // Defaults to "AES".
    String algorithm = YTGlobalConfiguration.CLIENT_CI_KEYALGORITHM.getValueAsString();
    // Defaults to "AES/CBC/PKCS5Padding".
    String transform = YTGlobalConfiguration.CLIENT_CI_CIPHERTRANSFORM.getValueAsString();
    String keystoreFile = YTGlobalConfiguration.CLIENT_CI_KEYSTORE_FILE.getValueAsString();
    String keystorePassword = YTGlobalConfiguration.CLIENT_CI_KEYSTORE_PASSWORD.getValueAsString();

    YTDocument jsonDoc = null;

    try {
      jsonDoc = new YTDocument().fromJSON(password, "noMap");
    } catch (Exception ex) {
      throw YTException.wrapException(
          new YTSecurityException("OSymmetricKeyCI.intercept() Exception: " + ex.getMessage()), ex);
    }

    // Override algorithm and transform, if they exist in the JSON document.
    if (jsonDoc.containsField("algorithm")) {
      algorithm = jsonDoc.field("algorithm");
    }
    if (jsonDoc.containsField("transform")) {
      transform = jsonDoc.field("transform");
    }

    // Just in case the default configuration gets changed, check it.
    if (transform == null || transform.isEmpty()) {
      throw new YTSecurityException(
          "OSymmetricKeyCI.intercept() cipher transformation is required");
    }

    // If the algorithm is not set, either as a default in the global configuration or in the JSON
    // document,
    // then determine the algorithm from the cipher transformation.
    if (algorithm == null) {
      algorithm = OSymmetricKey.separateAlgorithm(transform);
    }

    OSymmetricKey key = null;

    // "key" has priority over "keyFile" and "keyStore".
    if (jsonDoc.containsField("key")) {
      final String base64Key = jsonDoc.field("key");

      key = OSymmetricKey.fromString(algorithm, base64Key);
      key.setDefaultCipherTransform(transform);
    } else // "keyFile" has priority over "keyStore".
      if (jsonDoc.containsField("keyFile")) {
        key = OSymmetricKey.fromFile(algorithm, jsonDoc.field("keyFile"));
        key.setDefaultCipherTransform(transform);
      } else if (jsonDoc.containsField("keyStore")) {
        YTDocument ksDoc = jsonDoc.field("keyStore");

        if (ksDoc.containsField("file")) {
          keystoreFile = ksDoc.field("file");
        }

        if (keystoreFile == null || keystoreFile.isEmpty()) {
          throw new YTSecurityException("OSymmetricKeyCI.intercept() keystore file is required");
        }

        // Specific to Keystore, but override if present in the JSON document.
        if (ksDoc.containsField("password")) {
          keystorePassword = ksDoc.field("password");
        }

        String keyAlias = ksDoc.field("keyAlias");

        if (keyAlias == null || keyAlias.isEmpty()) {
          throw new YTSecurityException(
              "OSymmetricKeyCI.intercept() keystore key alias is required");
        }

        // keyPassword may be null.
        String keyPassword = ksDoc.field("keyPassword");

        // keystorePassword may be null.
        key = OSymmetricKey.fromKeystore(keystoreFile, keystorePassword, keyAlias, keyPassword);
        key.setDefaultCipherTransform(transform);
      } else {
        throw new YTSecurityException(
            "OSymmetricKeyCI.intercept() No suitable symmetric key property exists");
      }

    // This should never happen, but...
    if (key == null) {
      throw new YTSecurityException("OSymmetricKeyCI.intercept() OSymmetricKey is null");
    }

    encodedJSON = key.encrypt(transform, username);
  }
}
