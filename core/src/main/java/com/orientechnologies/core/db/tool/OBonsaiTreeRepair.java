package com.orientechnologies.core.db.tool;

import com.orientechnologies.core.command.OCommandOutputListener;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.OMetadata;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.record.impl.YTVertexInternal;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Find and repair broken bonsai tree removing the double linked buckets and regenerating the whole
 * tree with data from referring records.
 */
public class OBonsaiTreeRepair {

  public void repairDatabaseRidbags(YTDatabaseSessionInternal db,
      OCommandOutputListener outputListener) {
    message(outputListener, "Repair of ridbags is started ...\n");

    final OMetadata metadata = db.getMetadata();
    final YTSchema schema = metadata.getSchema();
    final YTClass edgeClass = schema.getClass("E");
    if (edgeClass != null) {
      final HashMap<String, Set<YTRID>> processedVertexes = new HashMap<String, Set<YTRID>>();
      final long countEdges = db.countClass(edgeClass.getName());

      message(outputListener, countEdges + " will be processed.");
      long counter = 0;

      for (YTEntityImpl edge : db.browseClass(edgeClass.getName())) {
        try {
          final String label;
          if (edge.field("label") != null) {
            label = edge.field("label");
          } else if (!edge.getClassName().equals(edgeClass.getName())) {
            label = edge.getClassName();
          } else {
            counter++;
            continue;
          }

          YTIdentifiable inId = edge.field("in");
          YTIdentifiable outId = edge.field("out");
          if (inId == null || outId == null) {
            db.delete(edge);
            continue;
          }
          final String inVertexName =
              YTVertexInternal.getEdgeLinkFieldName(ODirection.IN, label, true);
          final String outVertexName =
              YTVertexInternal.getEdgeLinkFieldName(ODirection.OUT, label, true);

          final YTEntityImpl inVertex = inId.getRecord();
          final YTEntityImpl outVertex = outId.getRecord();

          Set<YTRID> inVertexes = processedVertexes.get(inVertexName);
          if (inVertexes == null) {
            inVertexes = new HashSet<>();
            processedVertexes.put(inVertexName, inVertexes);
          }
          Set<YTRID> outVertexes = processedVertexes.get(outVertexName);
          if (outVertexes == null) {
            outVertexes = new HashSet<>();
            processedVertexes.put(outVertexName, outVertexes);
          }

          if (inVertex.field(inVertexName) instanceof RidBag) {
            if (inVertexes.add(inVertex.getIdentity())) {
              inVertex.field(inVertexName, new RidBag(db));
            }

            final RidBag inRidBag = inVertex.field(inVertexName);
            inRidBag.add(edge.getIdentity());

            inVertex.save();
          }

          if (outVertex.field(outVertexName) instanceof RidBag) {
            if (outVertexes.add(outVertex.getIdentity())) {
              outVertex.field(outVertexName, new RidBag(db));
            }

            final RidBag outRidBag = outVertex.field(outVertexName);
            outRidBag.add(edge.getIdentity());

            outVertex.save();
          }

          counter++;

          if (counter > 0 && counter % 1000 == 0) {
            message(
                outputListener, counter + " edges were processed out of " + countEdges + " \n.");
          }

        } catch (Exception e) {
          final StringWriter sw = new StringWriter();

          sw.append("Error during processing of edge with id ")
              .append(edge.getIdentity().toString())
              .append("\n");
          e.printStackTrace(new PrintWriter(sw));

          message(outputListener, sw.toString());
        }
      }

      message(outputListener, "Processed " + counter + " from " + countEdges + ".");
    }

    message(outputListener, "repair of ridbags is completed\n");
  }

  private void message(OCommandOutputListener outputListener, String message) {
    if (outputListener != null) {
      outputListener.onMessage(message);
    }
  }
}
