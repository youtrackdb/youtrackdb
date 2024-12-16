package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import java.util.Map;

/**
 * Drops a use.
 *
 * @since 4/22/2015
 */
public class CommandExecutorSQLDropUser extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_USER = "USER";

  private static final String SYNTAX = "DROP USER <user-name>";
  private static final String USER_CLASS = "OUser";
  private static final String USER_FIELD_NAME = "name";

  private String userName;

  @Override
  public CommandExecutorSQLDropUser parse(CommandRequest iRequest) {
    init((CommandRequestText) iRequest);

    parserRequiredKeyword(KEYWORD_DROP);
    parserRequiredKeyword(KEYWORD_USER);
    this.userName = parserRequiredWord(false, "Expected <user name>");

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs, DatabaseSessionInternal querySession) {
    if (this.userName == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    // Build following command:
    // DELETE FROM OUser WHERE name='<name>'

    //
    String sb =
        "DELETE FROM " + USER_CLASS + " WHERE " + USER_FIELD_NAME + "='" + this.userName + "'";

    //
    var db = getDatabase();
    return db.command(new CommandSQL(sb)).execute(db);
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
