package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence;
import java.util.Map;

/**
 * @since 3/5/2015
 */
public class CommandExecutorSQLAlterSequence extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_ALTER = "ALTER";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";
  public static final String KEYWORD_START = "START";
  public static final String KEYWORD_INCREMENT = "INCREMENT";
  public static final String KEYWORD_CACHE = "CACHE";

  private String sequenceName;
  private DBSequence.CreateParams params;

  @Override
  public CommandExecutorSQLAlterSequence parse(DatabaseSessionInternal session,
      CommandRequest iRequest) {
    final var textRequest = (CommandRequestText) iRequest;

    var queryText = textRequest.getText();
    var originalQuery = queryText;
    try {
      queryText = preParse(session, queryText, iRequest);
      textRequest.setText(queryText);

      init(session, (CommandRequestText) iRequest);

      parserRequiredKeyword(session.getDatabaseName(), KEYWORD_ALTER);
      parserRequiredKeyword(session.getDatabaseName(), KEYWORD_SEQUENCE);
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>",
          session.getDatabaseName());
      this.params = new DBSequence.CreateParams();

      String temp;
      while ((temp = parseOptionalWord(session.getDatabaseName(), true)) != null) {
        if (parserIsEnded()) {
          break;
        }

        if (temp.equals(KEYWORD_START)) {
          var startAsString = parserRequiredWord(true, "Expected <start value>",
              session.getDatabaseName());
          this.params.setStart(Long.parseLong(startAsString));
        } else if (temp.equals(KEYWORD_INCREMENT)) {
          var incrementAsString = parserRequiredWord(true, "Expected <increment value>",
              session.getDatabaseName());
          this.params.setIncrement(Integer.parseInt(incrementAsString));
        } else if (temp.equals(KEYWORD_CACHE)) {
          var cacheAsString = parserRequiredWord(true, "Expected <cache value>",
              session.getDatabaseName());
          this.params.setCacheSize(Integer.parseInt(cacheAsString));
        }
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  @Override
  public Object execute(DatabaseSessionInternal session, Map<Object, Object> iArgs) {
    if (this.sequenceName == null) {
      throw new CommandExecutionException(session,
          "Cannot execute the command because it has not been parsed yet");
    }

    var sequence = session.getMetadata().getSequenceLibrary()
        .getSequence(this.sequenceName);

    boolean result;
    try {
      result = sequence.updateParams(session, this.params);
      // TODO check, but reset should not be here
      //      sequence.reset();
    } catch (DatabaseException exc) {
      var message = "Unable to execute command: " + exc.getMessage();
      LogManager.instance().error(this, message, exc, (Object) null);
      throw new CommandExecutionException(session, message);
    }
    return result;
  }

  @Override
  public String getSyntax() {
    return "ALTER SEQUENCE <sequence> [START <value>] [INCREMENT <value>] [CACHE <value>]";
  }
}
