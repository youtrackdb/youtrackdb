package com.jetbrains.youtrack.db.internal.core.security;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.exception.YTSystemException;
import com.jetbrains.youtrack.db.internal.common.log.OLogManager;
import com.jetbrains.youtrack.db.internal.core.config.GlobalConfiguration;
import com.jetbrains.youtrack.db.internal.core.config.YTContextConfiguration;
import com.jetbrains.youtrack.db.internal.core.metadata.security.OToken;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OKeyProvider;
import com.jetbrains.youtrack.db.internal.core.metadata.security.jwt.OTokenHeader;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import javax.crypto.Mac;

public class OTokenSignImpl implements OTokenSign {

  public static final String ENCRYPTION_ALGORITHM_DEFAULT = "HmacSHA256";

  private String algorithm = ENCRYPTION_ALGORITHM_DEFAULT;

  private static final ThreadLocal<Map<String, Mac>> threadLocalMac = new MacThreadLocal();

  private final OKeyProvider keyProvider;

  private static class MacThreadLocal extends ThreadLocal<Map<String, Mac>> {

    @Override
    protected Map<String, Mac> initialValue() {
      return new HashMap<String, Mac>();
    }
  }

  public OTokenSignImpl(YTContextConfiguration config) {
    this(
        OTokenSignImpl.readKeyFromConfig(config),
        config.getValueAsString(GlobalConfiguration.NETWORK_TOKEN_ENCRYPTION_ALGORITHM));
  }

  public OTokenSignImpl(byte[] key, String algorithm) {
    this.keyProvider = new DefaultKeyProvider(key);
    if (algorithm != null) {
      this.algorithm = algorithm;
    }
    try {
      Mac.getInstance(this.algorithm);
    } catch (NoSuchAlgorithmException nsa) {
      throw new IllegalArgumentException(
          "Can't find encryption algorithm '" + algorithm + "'", nsa);
    }
  }

  private Mac getLocalMac() {
    Map<String, Mac> map = threadLocalMac.get();
    Mac mac = map.get(this.algorithm);
    if (mac == null) {
      try {
        mac = Mac.getInstance(this.algorithm);
      } catch (NoSuchAlgorithmException nsa) {
        throw new IllegalArgumentException(
            "Can't find encryption algorithm '" + algorithm + "'", nsa);
      }
      map.put(this.algorithm, mac);
    }
    return mac;
  }

  @Override
  public byte[] signToken(final OTokenHeader header, final byte[] unsignedToken) {
    final Mac mac = getLocalMac();
    try {
      mac.init(keyProvider.getKey(header));
      return mac.doFinal(unsignedToken);
    } catch (Exception ex) {
      throw YTException.wrapException(new YTSystemException("Error on token parsing"), ex);
    } finally {
      mac.reset();
    }
  }

  @Override
  public boolean verifyTokenSign(OParsedToken parsed) {
    OToken token = parsed.getToken();
    byte[] tokenBytes = parsed.getTokenBytes();
    byte[] signature = parsed.getSignature();
    final Mac mac = getLocalMac();

    try {
      mac.init(keyProvider.getKey(token.getHeader()));
      mac.update(tokenBytes, 0, tokenBytes.length);
      final byte[] calculatedSignature = mac.doFinal();
      boolean valid = MessageDigest.isEqual(calculatedSignature, signature);
      if (!valid) {
        OLogManager.instance()
            .warn(
                this,
                "Token signature failure: %s",
                Base64.getEncoder().encodeToString(tokenBytes));
      }
      return valid;

    } catch (RuntimeException e) {
      throw e;
    } catch (Exception e) {
      throw YTException.wrapException(new YTSystemException("Token signature cannot be verified"),
          e);
    } finally {
      mac.reset();
    }
  }

  @Override
  public String getAlgorithm() {
    return algorithm;
  }

  @Override
  public String getDefaultKey() {
    return this.keyProvider.getDefaultKey();
  }

  @Override
  public String[] getKeys() {
    return this.keyProvider.getKeys();
  }

  public static byte[] readKeyFromConfig(YTContextConfiguration config) {
    byte[] key = null;
    String configKey = config.getValueAsString(GlobalConfiguration.NETWORK_TOKEN_SECRETKEY);
    if (configKey == null || configKey.length() == 0) {
      if (configKey != null && configKey.length() > 0) {
        key = Base64.getUrlDecoder().decode(configKey);
      }
    }

    if (key == null) {
      key = OSecurityManager.digestSHA256(String.valueOf(new SecureRandom().nextLong()));
    }
    return key;
  }
}
