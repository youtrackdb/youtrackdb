package com.orientechnologies.core.db.tool;

import com.orientechnologies.core.OCreateDatabaseUtil;
import com.orientechnologies.core.command.OCommandOutputListener;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.YouTrackDB;
import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTVertex;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.Assert;

public class ODatabaseSuperNodeTest {

  public static void main(String[] args) {
    final ODatabaseSuperNodeTest tester = new ODatabaseSuperNodeTest();

    final List<Integer> numberEdges = Arrays.asList(100000, 500000, 1000000, 5000000);
    final List<Long> exportStats = new ArrayList<>(numberEdges.size());
    final List<Long> importStats = new ArrayList<>(numberEdges.size());

    for (int numberEdge : numberEdges) {
      final String databaseName = "superNode_export";
      final String exportDbUrl =
          "embedded:target/export_" + ODatabaseSuperNodeTest.class.getSimpleName();
      YouTrackDB youTrackDB =
          OCreateDatabaseUtil.createDatabase(
              databaseName, exportDbUrl, OCreateDatabaseUtil.TYPE_MEMORY);

      final ByteArrayOutputStream output = new ByteArrayOutputStream();
      try {
        testExportDatabase(numberEdge, exportStats, databaseName, youTrackDB, output);
        Thread.sleep(2000);
      } catch (final InterruptedException e) {
        e.printStackTrace();
      } finally {
        youTrackDB.drop(databaseName);
        youTrackDB.close();
      }

      final String importDbUrl =
          "memory:target/import_" + ODatabaseSuperNodeTest.class.getSimpleName();
      youTrackDB =
          OCreateDatabaseUtil.createDatabase(
              databaseName + "_reImport", importDbUrl, OCreateDatabaseUtil.TYPE_PLOCAL);
      try {
        testImportDatabase(numberEdge, databaseName, youTrackDB, output, importStats);
      } finally {
        youTrackDB.drop(databaseName + "_reImport");
        youTrackDB.close();
      }
    }

    for (int i = 0; i < exportStats.size(); i++) {
      System.out.println("Export-" + numberEdges.get(i) + "(ms)=" + exportStats.get(i));
      System.out.println("Import-" + numberEdges.get(i) + "(ms)=" + importStats.get(i));
    }
  }

  private static void testExportDatabase(
      final int edgeNumber,
      final List<Long> stats,
      final String databaseName,
      final YouTrackDB youTrackDB,
      final OutputStream output) {

    try (final YTDatabaseSession session =
        youTrackDB.open(databaseName, "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      session.createClassIfNotExist("SuperNodeClass", "V");
      session.createClassIfNotExist("NonSuperEdgeClass", "E");

      // session.begin();
      final YTVertex fromNode = session.newVertex("SuperNodeClass");
      fromNode.save();
      final YTVertex toNode = session.newVertex("SuperNodeClass");
      toNode.save();

      for (int i = 0; i < edgeNumber; i++) {
        final YTEdge edge = session.newEdge(fromNode, toNode, "NonSuperEdgeClass");
        edge.save();
      }
      session.commit();

      final ODatabaseExport export =
          new ODatabaseExport(
              (YTDatabaseSessionInternal) session,
              output,
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                }
              });
      // export.setOptions(" -excludeAll -includeSchema=true");
      // List of export options can be found in `ODatabaseImpExpAbstract`
      export.setOptions(" -includeSchema=true");
      final long start = System.nanoTime();
      export.exportDatabase();
      final long time = (System.nanoTime() - start) / 1000000;
      stats.add(time);
      System.out.println("Export-" + edgeNumber + "(ms)=" + time);
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }

  private static void testImportDatabase(
      int numberEdge,
      final String databaseName,
      final YouTrackDB youTrackDB,
      final ByteArrayOutputStream output,
      List<Long> stats) {
    try (var db =
        (YTDatabaseSessionInternal) youTrackDB.open(
            databaseName + "_reImport", "admin", OCreateDatabaseUtil.NEW_ADMIN_PASSWORD)) {
      final ODatabaseImport importer =
          new ODatabaseImport(
              db,
              new ByteArrayInputStream(output.toByteArray()),
              new OCommandOutputListener() {
                @Override
                public void onMessage(String iText) {
                }
              });
      final long start = System.nanoTime();
      importer.importDatabase();
      final long time = (System.nanoTime() - start) / 1000000;
      stats.add(time);
      System.out.println("Import-" + numberEdge + "(ms)=" + time);
      Assert.assertTrue(db.getMetadata().getSchema().existsClass("SuperNodeClass"));
    } catch (final IOException e) {
      e.printStackTrace();
    }
  }
}
