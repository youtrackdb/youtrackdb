package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.command.OCommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.db.YTDatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.exception.YTCommandExecutionException;
import com.jetbrains.youtrack.db.internal.core.exception.YTDatabaseException;
import java.util.Map;

/**
 * @since 2/28/2015
 */
public class CommandExecutorSQLDropSequence extends CommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";

  private String sequenceName;

  @Override
  public CommandExecutorSQLDropSequence parse(CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      final YTDatabaseSessionInternal database = getDatabase();
      final StringBuilder word = new StringBuilder();

      parserRequiredKeyword("DROP");
      parserRequiredKeyword("SEQUENCE");
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    if (this.sequenceName == null) {
      throw new YTCommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final var database = getDatabase();
    try {
      database.getMetadata().getSequenceLibrary().dropSequence(this.sequenceName);
    } catch (YTDatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      LogManager.instance().error(this, message, exc, (Object) null);
      throw new YTCommandExecutionException(message);
    }
    return true;
  }

  @Override
  public String getSyntax() {
    return "DROP SEQUENCE <sequence>";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
