package com.orientechnologies.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.core.command.OCommandRequest;
import com.orientechnologies.core.command.OCommandRequestText;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.exception.YTCommandExecutionException;
import com.orientechnologies.core.exception.YTDatabaseException;
import java.util.Map;

/**
 * @since 2/28/2015
 */
public class OCommandExecutorSQLDropSequence extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_DROP = "DROP";
  public static final String KEYWORD_SEQUENCE = "SEQUENCE";

  private String sequenceName;

  @Override
  public OCommandExecutorSQLDropSequence parse(OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);

      init((OCommandRequestText) iRequest);

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
      OLogManager.instance().error(this, message, exc, (Object) null);
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
