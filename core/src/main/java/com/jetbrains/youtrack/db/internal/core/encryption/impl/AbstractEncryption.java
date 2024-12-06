package com.jetbrains.youtrack.db.internal.core.encryption.impl;

import com.jetbrains.youtrack.db.internal.common.exception.BaseException;
import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.exception.InvalidStorageEncryptionKeyException;
import javax.crypto.Cipher;

/**
 * * (https://docs.oracle.com/javase/7/docs/technotes/guides/security/SunProviders.html). Issue
 * https://github.com/orientechnologies/orientdb/issues/89.
 */
public abstract class AbstractEncryption implements Encryption {

  /**
   * *
   *
   * @param mode   it can be Cipher.ENCRYPT_MODE or Cipher.DECRYPT_MODE
   * @param input
   * @param offset
   * @param length
   * @return
   * @throws Throwable
   */
  public abstract byte[] encryptOrDecrypt(int mode, byte[] input, int offset, int length)
      throws Exception;

  @Override
  public byte[] encrypt(final byte[] content) {
    return encrypt(content, 0, content.length);
  }

  @Override
  public byte[] decrypt(final byte[] content) {
    return decrypt(content, 0, content.length);
  }

  @Override
  public byte[] encrypt(final byte[] content, final int offset, final int length) {
    try {
      return encryptOrDecrypt(Cipher.ENCRYPT_MODE, content, offset, length);
    } catch (Exception e) {
      throw BaseException.wrapException(
          new InvalidStorageEncryptionKeyException("Cannot encrypt content"), e);
    }
  }

  @Override
  public byte[] decrypt(final byte[] content, final int offset, final int length) {
    try {
      return encryptOrDecrypt(Cipher.DECRYPT_MODE, content, offset, length);
    } catch (Exception e) {
      throw BaseException.wrapException(
          new InvalidStorageEncryptionKeyException("Cannot decrypt content"), e);
    }
  }
}
