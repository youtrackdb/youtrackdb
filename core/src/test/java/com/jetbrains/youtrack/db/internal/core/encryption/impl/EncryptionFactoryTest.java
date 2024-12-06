package com.jetbrains.youtrack.db.internal.core.encryption.impl;

import com.jetbrains.youtrack.db.internal.core.encryption.Encryption;
import com.jetbrains.youtrack.db.internal.core.encryption.EncryptionFactory;
import java.util.Base64;
import org.junit.Assert;
import org.junit.Test;

public class EncryptionFactoryTest {

  @Test
  public void test() {
    String key =
        new String(
            Base64.getEncoder()
                .encode(new byte[]{1, 2, 3, 4, 5, 6, 7, 8, 1, 2, 3, 4, 5, 6, 7, 8}));
    Encryption enc = EncryptionFactory.INSTANCE.getEncryption(AESGCMEncryption.NAME, key);
    Assert.assertEquals(AESGCMEncryption.class, enc.getClass());
  }
}
