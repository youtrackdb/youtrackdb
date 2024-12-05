package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.OCommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import java.util.Map;

/**
 * Drops a use.
 *
 * @since 4/22/2015
 */
public class OCommandExecutorSQLDropUser extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_USER = "USER";

  private static final String SYNTAX = "DROP USER <user-name>";
  private static final String USER_CLASS = "OUser";
  private static final String USER_FIELD_NAME = "name";

  private String userName;

  @Override
  public OCommandExecutorSQLDropUser parse(OCommandRequest iRequest) {
    init((OCommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_DROP);
    parserRequiredKeyword(KEYWORD_USER);
    this.userName = parserRequiredWord(false, "Expected <user name>");

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (this.userName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    // Build following command:
    // DELETE FROM OUser WHERE name='<name>'

    //
    String sb =
        "DELETE FROM " + USER_CLASS + " WHERE " + USER_FIELD_NAME + "='" + this.userName + "'";

    //
    var db = getDatabase();
    return db.command(new OCommandSQL(sb)).execute(db);
  }

  @Override
  public String getSyntax() {
    return SYNTAX;
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
