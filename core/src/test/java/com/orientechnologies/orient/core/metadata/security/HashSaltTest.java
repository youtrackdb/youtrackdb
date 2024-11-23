package com.orientechnologies.orient.core.metadata.security;

import com.orientechnologies.orient.core.security.OSecurityManager;
import java.security.NoSuchAlgorithmException;
import java.security.spec.InvalidKeySpecException;
import org.junit.Assert;
import org.junit.Test;

/**
 * Tests the salt + hash of passwords.
 */
public class HashSaltTest {

  @Test
  public void testSalt() throws InvalidKeySpecException, NoSuchAlgorithmException {
    final String password = "OrientDBisCool";
    final OSecurityManager sm = new OSecurityManager();
    final String hashed = OSecurityManager.createHashWithSalt(password);

    Assert.assertTrue(OSecurityManager.checkPasswordWithSalt(password, hashed));
  }
}
