package com.orientechnologies.orient.core.sql;

import com.orientechnologies.common.exception.YTException;
import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.orient.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.orient.core.command.OCommandRequest;
import com.orientechnologies.orient.core.command.OCommandRequestText;
import com.orientechnologies.orient.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.orient.core.exception.YTCommandExecutionException;
import com.orientechnologies.orient.core.exception.YTDatabaseException;
import com.orientechnologies.orient.core.metadata.sequence.OSequenceHelper;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence;
import com.orientechnologies.orient.core.metadata.sequence.YTSequence.SEQUENCE_TYPE;
import java.util.Arrays;
import java.util.Map;

/**
 * @since 2/28/2015
 */
public class OCommandExecutorSQLCreateSequence extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_CREATE = "CREATE";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";
  public static final String KEYWORD_TYPE = "TYPE";
  public static final String KEYWORD_START = "START";
  public static final String KEYWORD_INCREMENT = "INCREMENT";
  public static final String KEYWORD_CACHE = "CACHE";

  private String sequenceName;
  private SEQUENCE_TYPE sequenceType;
  private YTSequence.CreateParams params;

  @Override
  public OCommandExecutorSQLCreateSequence parse(OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

      parserRequiredKeyword(KEYWORD_CREATE);
      parserRequiredKeyword(KEYWORD_SEQUENCE);
      this.sequenceName = parserRequiredWord(false, "Expected <sequence name>");
      this.params = new YTSequence.CreateParams().setDefaults();

      String temp;
      while ((temp = parseOptionalWord(true)) != null) {
        if (parserIsEnded()) {
          break;
        }

        if (temp.equals(KEYWORD_TYPE)) {
          String typeAsString = parserRequiredWord(true, "Expected <sequence type>");
          try {
            this.sequenceType = OSequenceHelper.getSequenceTyeFromString(typeAsString);
          } catch (IllegalArgumentException e) {
            throw YTException.wrapException(
                new YTCommandSQLParsingException(
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
        this.sequenceType = OSequenceHelper.DEFAULT_SEQUENCE_TYPE;
      }
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
      database
          .getMetadata()
          .getSequenceLibrary()
          .createSequence(this.sequenceName, this.sequenceType, this.params);
    } catch (YTDatabaseException exc) {
      String message = "Unable to execute command: " + exc.getMessage();
      OLogManager.instance().error(this, message, exc, (Object) null);
      throw new YTCommandExecutionException(message);
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
