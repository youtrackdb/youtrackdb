package com.jetbrains.youtrack.db.internal.core.security.symmetrickey;

import static org.assertj.core.api.Assertions.assertThat;

import com.jetbrains.youtrack.db.internal.DbTestBase;
import com.jetbrains.youtrack.db.api.DatabaseSession;
import org.junit.Test;

/**
 *
 */
public class SymmetricKeyTest extends DbTestBase {

  private void command(DatabaseSession db, String sql, Object... params) {
    db.command(sql, params).close();
  }

  @Test
  public void shouldTestDefaultConstructor() throws Exception {
    SymmetricKey sk = new SymmetricKey();

    String msgToEncrypt = "Please, encrypt this!";

    String magic = sk.encrypt(msgToEncrypt);

    String decryptedMsg = sk.decryptAsString(magic);

    System.out.println("decryptedMsg = " + decryptedMsg);

    assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  @Test
  public void shouldTestSpecificAESKey() throws Exception {
    SymmetricKey sk = new SymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    String msgToEncrypt = "Please, encrypt this!";

    String magic = sk.encrypt("AES/CBC/PKCS5Padding", msgToEncrypt);

    String decryptedMsg = sk.decryptAsString(magic);

    System.out.println("decryptedMsg = " + decryptedMsg);

    assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  @Test
  public void shouldTestGeneratedAESKey() throws Exception {
    SymmetricKey sk = new SymmetricKey("AES", "AES/CBC/PKCS5Padding", 128);

    String key = sk.getBase64Key();

    String msgToEncrypt = "Please, encrypt this!";

    String magic = sk.encrypt(msgToEncrypt);

    SymmetricKey sk2 = new SymmetricKey("AES", key);

    String decryptedMsg = sk2.decryptAsString(magic);

    assertThat(msgToEncrypt).isEqualTo(decryptedMsg);
  }

  /* Fails under develop
  @Test
  public void shouldTestOSymmetricKeySecurity() throws Exception {
    DatabaseDocumentTx db = new DatabaseDocumentTx("memory:" + SymmetricKeyTest.class.getSimpleName());

    if (db.exists()) {
      db.open("admin", "admin");
      db.drop();
    }

    db.create();

    final String user = "test";

    command(db, "insert into OUser set name=?, password='password', status='ACTIVE', roles=(SELECT FROM Role WHERE name = ?)", user, "admin");
    command(db, "update OUser set properties={'@type':'d', 'key':'8BC7LeGkFbmHEYNTz5GwDw==','keyAlgorithm':'AES'} where name = ?", user);

    db.close();

    db.setProperty(ODatabase.OPTIONS.SECURITY.toString(), SymmetricKeySecurity.class);

    SymmetricKey sk = new SymmetricKey("AES", "8BC7LeGkFbmHEYNTz5GwDw==");

    // We encrypt the username and specify the Base64-encoded JSON document as the password.
    db.open(user, sk.encrypt("AES/CBC/PKCS5Padding", user));
    db.close();
  } */
}
