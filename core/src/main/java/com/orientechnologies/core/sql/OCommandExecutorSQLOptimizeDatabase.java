/*
 *
 *
 *  *
 *  *  Licensed under the Apache License, Version 2.0 (the "License");
 *  *  you may not use this file except in compliance with the License.
 *  *  You may obtain a copy of the License at
 *  *
 *  *       http://www.apache.org/licenses/LICENSE-2.0
 *  *
 *  *  Unless required by applicable law or agreed to in writing, software
 *  *  distributed under the License is distributed on an "AS IS" BASIS,
 *  *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  *  See the License for the specific language governing permissions and
 *  *  limitations under the License.
 *  *
 *
 *
 */
package com.orientechnologies.core.sql;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.command.OCommandDistributedReplicateRequest;
import com.orientechnologies.core.command.OCommandRequest;
import com.orientechnologies.core.command.OCommandRequestText;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import java.util.Iterator;
import java.util.Map;

/**
 * SQL ALTER DATABASE command: Changes an attribute of the current database.
 */
@SuppressWarnings("unchecked")
public class OCommandExecutorSQLOptimizeDatabase extends OCommandExecutorSQLAbstract
    implements OCommandDistributedReplicateRequest {

  public static final String KEYWORD_OPTIMIZE = "OPTIMIZE";
  public static final String KEYWORD_DATABASE = "DATABASE";
  public static final String KEYWORD_EDGE = "-LWEDGES";
  public static final String KEYWORD_NOVERBOSE = "-NOVERBOSE";

  private boolean optimizeEdges = false;
  private boolean verbose = true;
  private final int batch = 1000;

  public OCommandExecutorSQLOptimizeDatabase parse(final OCommandRequest iRequest) {
    final OCommandRequestText textRequest = (OCommandRequestText) iRequest;

    String queryText = textRequest.getText();
    String originalQuery = queryText;
    try {
      queryText = preParse(queryText, iRequest);
      textRequest.setText(queryText);
      init((OCommandRequestText) iRequest);

      StringBuilder word = new StringBuilder();

      int oldPos = 0;
      int pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_OPTIMIZE)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_OPTIMIZE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      oldPos = pos;
      pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
      if (pos == -1 || !word.toString().equals(KEYWORD_DATABASE)) {
        throw new YTCommandSQLParsingException(
            "Keyword " + KEYWORD_DATABASE + " not found. Use " + getSyntax(), parserText, oldPos);
      }

      while (!parserIsEnded() && word.length() > 0) {
        oldPos = pos;
        pos = nextWord(parserText, parserTextUpperCase, oldPos, word, true);
        if (word.toString().equals(KEYWORD_EDGE)) {
          optimizeEdges = true;
        } else if (word.toString().equals(KEYWORD_NOVERBOSE)) {
          verbose = false;
        }
      }
    } finally {
      textRequest.setText(originalQuery);
    }

    return this;
  }

  /**
   * Execute the ALTER DATABASE.
   */
  public Object execute(final Map<Object, Object> iArgs, YTDatabaseSessionInternal querySession) {
    final StringBuilder result = new StringBuilder();

    if (optimizeEdges) {
      result.append(optimizeEdges());
    }

    return result.toString();
  }

  private String optimizeEdges() {
    final YTDatabaseSessionInternal db = getDatabase();

    long transformed = 0;
    if (db.getTransaction().isActive()) {
      db.commit();
    }

    db.begin();

    try {

      final long totalEdges = db.countClass("E");
      long browsedEdges = 0;
      long lastLapBrowsed = 0;
      long lastLapTime = System.currentTimeMillis();

      for (YTEntityImpl doc : db.browseClass("E")) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }

        browsedEdges++;

        if (doc != null) {
          if (doc.fields() == 2) {
            final YTRID edgeIdentity = doc.getIdentity();

            final YTEntityImpl outV = doc.field("out");
            final YTEntityImpl inV = doc.field("in");

            // OUTGOING
            final Object outField = outV.field("out_" + doc.getClassName());
            if (outField instanceof RidBag) {
              final Iterator<YTIdentifiable> it = ((RidBag) outField).iterator();
              while (it.hasNext()) {
                YTIdentifiable v = it.next();
                if (edgeIdentity.equals(v)) {
                  // REPLACE EDGE RID WITH IN-VERTEX RID
                  it.remove();
                  ((RidBag) outField).add(inV.getIdentity());
                  break;
                }
              }
            }

            outV.save();

            // INCOMING
            final Object inField = inV.field("in_" + doc.getClassName());
            if (outField instanceof RidBag) {
              final Iterator<YTIdentifiable> it = ((RidBag) inField).iterator();
              while (it.hasNext()) {
                YTIdentifiable v = it.next();
                if (edgeIdentity.equals(v)) {
                  // REPLACE EDGE RID WITH IN-VERTEX RID
                  it.remove();
                  ((RidBag) inField).add(outV.getIdentity());
                  break;
                }
              }
            }

            inV.save();

            doc.delete();

            if (++transformed % batch == 0) {
              db.commit();
              db.begin();
            }

            final long now = System.currentTimeMillis();

            if (verbose && (now - lastLapTime > 2000)) {
              final long elapsed = now - lastLapTime;

              OLogManager.instance()
                  .info(
                      this,
                      "Browsed %,d of %,d edges, transformed %,d so far (%,d edges/sec)",
                      browsedEdges,
                      totalEdges,
                      transformed,
                      (((browsedEdges - lastLapBrowsed) * 1000 / elapsed)));

              lastLapTime = System.currentTimeMillis();
              lastLapBrowsed = browsedEdges;
            }
          }
        }
      }

      // LAST COMMIT
      db.commit();

    } finally {
      if (db.getTransaction().isActive()) {
        db.rollback();
      }
    }
    return "Transformed " + transformed + " regular edges in lightweight edges";
  }

  @Override
  public QUORUM_TYPE getQuorumType() {
    return QUORUM_TYPE.ALL;
  }

  public String getSyntax() {
    return "OPTIMIZE DATABASE [-lwedges]";
  }
}
