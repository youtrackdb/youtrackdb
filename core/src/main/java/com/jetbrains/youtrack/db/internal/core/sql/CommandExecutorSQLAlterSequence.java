package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.Sequence;
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
  private Sequence.CreateParams params;

  @Override
  public CommandExecutorSQLAlterSequence parse(DatabaseSessionInternal db,
      CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      final DatabaseSessionInternal database = getDatabase();
      final StringBuilder word = new StringBuilder();

      parserRequiredKeyword(KEYWORD_ALTER);
      parserRequiredKeyword(KEYWORD_SEQUENCE);
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
      this.params = new Sequence.CreateParams();

      String temp;
      while ((temp = parseOptionalWord(true)) != null) {
        if (parserIsEnded()) {
          break;
        }

        if (temp.equals(KEYWORD_START)) {
          String startAsString = parserRequiredWord(true, "Expected <start value>");
          this.params.setStart(Long.parseLong(startAsString));
        } else if (temp.equals(KEYWORD_INCREMENT)) {
          String incrementAsString = parserRequiredWord(true, "Expected <increment value>");
          this.params.setIncrement(Integer.parseInt(incrementAsString));
        } else if (temp.equals(KEYWORD_CACHE)) {
          String cacheAsString = parserRequiredWord(true, "Expected <cache value>");
          this.params.setCacheSize(Integer.parseInt(cacheAsString));
        }
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  @Override
  public Object execute(DatabaseSessionInternal db, Map<Object, Object> iArgs) {
    if (this.sequenceName == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    Sequence sequence = db.getMetadata().getSequenceLibrary()
        .getSequence(this.sequenceName);

    boolean result;
    try {
      result = sequence.updateParams(db, this.params);
      // TODO check, but reset should not be here
      //      sequence.reset();
    } catch (DatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      LogManager.instance().error(this, message, exc, (Object) null);
      throw new CommandExecutionException(message);
    }
    return result;
  }

  @Override
  public String getSyntax() {
    return "ALTER SEQUENCE <sequence> [START <value>] [INCREMENT <value>] [CACHE <value>]";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
