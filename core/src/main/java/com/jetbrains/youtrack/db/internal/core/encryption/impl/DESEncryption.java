package com.jetbrains.youtrack.db.internal.core.encryption.impl;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.exception.InvalidStorageEncryptionKeyException;
import com.jetbrains.youtrack.db.internal.core.exception.SecurityException;
import java.util.Base64;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.SecretKeyFactory;
import javax.crypto.spec.DESKeySpec;

public class DESEncryption extends AbstractEncryption {

  // @see
  // https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html#SunJCEProvider
  private static final String TRANSFORMATION =
      "DES/ECB/PKCS5Padding"; // //we use ECB because we cannot
  private static final String ALGORITHM_NAME = "DES";

  private SecretKey theKey;
  private Cipher cipher;

  private boolean initialized = false;

  public static final String NAME = "des";

  @Override
  public String name() {
    return NAME;
  }

  public DESEncryption() {
  }

  public Encryption configure(final String iOptions) {
    initialized = false;

    if (iOptions == null) {
      throw new SecurityException(
          "DES encryption has been selected, but no key was found. Please configure it by passing"
              + " the key as property at database create/open. The property key is: '"
              + GlobalConfiguration.STORAGE_ENCRYPTION_KEY.getKey()
              + "'");
    }

    try {
      final byte[] key = Base64.getDecoder().decode(iOptions);

      final DESKeySpec desKeySpec = new DESKeySpec(key);
      final SecretKeyFactory keyFactory = SecretKeyFactory.getInstance(ALGORITHM_NAME);

      theKey = keyFactory.generateSecret(desKeySpec);
      cipher = Cipher.getInstance(TRANSFORMATION);

    } catch (Exception e) {
      throw BaseException.wrapException(
          new InvalidStorageEncryptionKeyException(
              "Cannot initialize DES encryption with current key. Assure the key is a BASE64 - 64"
                  + " bits long"),
          e);
    }

    this.initialized = true;

    return this;
  }

  public byte[] encryptOrDecrypt(
      final int mode, final byte[] input, final int offset, final int length) throws Exception {
    if (!initialized) {
      throw new SecurityException("DES encryption algorithm is not available");
    }

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
