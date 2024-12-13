package com.jetbrains.youtrack.db.internal.core.encryption.impl;

import com.jetbrains.youtrack.db.api.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.SecurityException;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.exception.InvalidStorageEncryptionKeyException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.spec.SecretKeySpec;

public class AESEncryption extends AbstractEncryption {

  // @see
  // https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private static final String TRANSFORMATION =
      "AES/ECB/PKCS5Padding"; // we use ECB because we cannot store the
  private static final String ALGORITHM_NAME = "AES";

  private SecretKeySpec theKey;

  private boolean initialized = false;

  public static final String NAME = "aes";

  @Override
  public String name() {
    return NAME;
  }

  public AESEncryption() {
  }

  public Encryption configure(final String iOptions) {
    initialized = false;

    if (iOptions == null) {
      throw new SecurityException(
          "AES encryption has been selected, but no key was found. Please configure it by passing"
              + " the key as property at database create/open. The property key is: '"
              + GlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey()
              + "'");
    }

    try {
      final byte[] key = Base64.getDecoder().decode(iOptions);

      theKey = new SecretKeySpec(key, ALGORITHM_NAME); // AES

    } catch (Exception e) {
      throw BaseException.wrapException(
          new InvalidStorageEncryptionKeyException(
              "Cannot initialize AES encryption with current key. Assure the key is a BASE64 - 128"
                  + " oe 256 bits long"),
          e);
    }

    this.initialized = true;

    return this;
  }

  public byte[] encryptOrDecrypt(
      final int mode, final byte[] input, final int offset, final int length) throws Exception {
    if (!initialized) {
      throw new SecurityException("AES encryption algorithm is not available");
    }
    Cipher cipher = Cipher.getInstance(TRANSFORMATION);
    cipher.init(mode, theKey);

    final byte[] content;
    if (offset == 0 && length == input.length) {
      content = input;
    } else {
      content = new byte[length];
      System.arraycopy(input, offset, content, 0, length);
    }
    return cipher.doFinal(content);
  }
}
