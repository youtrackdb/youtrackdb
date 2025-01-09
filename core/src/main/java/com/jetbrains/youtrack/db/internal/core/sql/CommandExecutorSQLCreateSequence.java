package com.jetbrains.youtrack.db.internal.core.sql;

import com.jetbrains.youtrack.db.api.exception.BaseException;
import com.jetbrains.youtrack.db.api.exception.CommandExecutionException;
import com.jetbrains.youtrack.db.api.exception.CommandSQLParsingException;
import com.jetbrains.youtrack.db.api.exception.DatabaseException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.command.CommandDistributedReplicateRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequest;
import com.jetbrains.youtrack.db.internal.core.command.CommandRequestText;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.DBSequence.SEQUENCE_TYPE;
import com.jetbrains.youtrack.db.internal.core.metadata.sequence.SequenceHelper;
import java.util.Arrays;
import java.util.Map;

/**
 * @since 2/28/2015
 */
public class CommandExecutorSQLCreateSequence extends CommandExecutorSQLAbstract
    implements CommandDistributedReplicateRequest {

  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";
  public static final String KEYWORD_TYPE = "TYPE";
  public static final String KEYWORD_START = "START";
  public static final String KEYWORD_INCREMENT = "INCREMENT";
  public static final String KEYWORD_CACHE = "CACHE";

  private String sequenceName;
  private SEQUENCE_TYPE sequenceType;
  private DBSequence.CreateParams params;

  @Override
  public CommandExecutorSQLCreateSequence parse(CommandRequest iRequest) {
    final CommandRequestText textRequest = (CommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((CommandRequestText) iRequest);

      parserRequiredKeyword(KEYWORD_CREATE);
      parserRequiredKeyword(KEYWORD_SEQUENCE);
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
      this.params = new DBSequence.CreateParams().setDefaults();

      String temp;
      while ((temp = parseOptionalWord(true)) != null) {
        if (parserIsEnded()) {
          break;
        }

        if (temp.equals(KEYWORD_TYPE)) {
          String typeAsString = parserRequiredWord(true, "Expected <sequence type>");
          try {
            this.sequenceType = SequenceHelper.getSequenceTyeFromString(typeAsString);
          } catch (IllegalArgumentException e) {
            throw BaseException.wrapException(
                new CommandSQLParsingException(
                    "Unknown sequence type '"
                        + typeAsString
                        + "'. Supported attributes are: "
                        + Arrays.toString(SEQUENCE_TYPE.values())),
                e);
          }
        } else if (temp.equals(KEYWORD_START)) {
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

      if (this.sequenceType == null) {
        this.sequenceType = SequenceHelper.DEFAULT_SEQUENCE_TYPE;
      }
    } finally {
      textRequest.setText(originalQuery);
    }
    return this;
  }

  @Override
  public Object execute(Map<Object, Object> iArgs, DatabaseSessionInternal querySession) {
    if (this.sequenceName == null) {
      throw new CommandExecutionException(
          "Cannot execute the command because it has not been parsed yet");
    }

    final var database = getDatabase();

    try {
      database
          .getMetadata()
          .getSequenceLibrary()
          .createSequence(this.sequenceName, this.sequenceType, this.params);
    } catch (DatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      LogManager.instance().error(this, message, exc, (Object) null);
      throw new CommandExecutionException(message);
    }

    return database.getMetadata().getSequenceLibrary().getSequenceCount();
  }

  @Override
  public String getSyntax() {
    return "CREATE SEQUENCE <sequence> [TYPE <CACHED|ORDERED>] [START <value>] [INCREMENT <value>]"
        + " [CACHE <value>]";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }
}
