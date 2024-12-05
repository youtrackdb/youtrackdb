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
package com.orientechnologies.core.db.tool;

import static com.orientechnologies.core.record.impl.ODocumentHelper.makeDbCall;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.core.command.OCommandOutputListener;
import com.orientechnologies.core.config.OStorageConfiguration;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.id.YTRecordId;
import com.orientechnologies.core.index.OIndex;
import com.orientechnologies.core.index.OIndexInternal;
import com.orientechnologies.core.index.OIndexManagerAbstract;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.impl.ODocumentHelper;
import com.orientechnologies.core.record.impl.ODocumentHelper.ODbRelatedCall;
import com.orientechnologies.core.record.impl.YTEntityImpl;
import com.orientechnologies.core.sql.executor.YTResultSet;
import com.orientechnologies.core.storage.OPhysicalPosition;
import com.orientechnologies.core.storage.ORawBuffer;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

public class ODatabaseCompare extends ODatabaseImpExpAbstract {

  private final YTDatabaseSessionInternal databaseOne;
  private final YTDatabaseSessionInternal databaseTwo;

  private boolean compareEntriesForAutomaticIndexes = false;
  private boolean autoDetectExportImportMap = true;

  private int differences = 0;
  private boolean compareIndexMetadata = false;

  private final Set<String> excludeIndexes = new HashSet<>();

  private int clusterDifference = 0;

  public ODatabaseCompare(
      YTDatabaseSessionInternal databaseOne,
      YTDatabaseSessionInternal databaseTwo,
      final OCommandOutputListener iListener) {
    super(null, null, iListener);

    listener.onMessage(
        "\nComparing two local databases:\n1) "
            + databaseOne.getURL()
            + "\n2) "
            + databaseTwo.getURL()
            + "\n");

    this.databaseOne = databaseOne;

    this.databaseTwo = databaseTwo;

    // exclude automatically generated clusters
    excludeIndexes.add(ODatabaseImport.EXPORT_IMPORT_INDEX_NAME);

    final YTSchema schema = databaseTwo.getMetadata().getSchema();
    final YTClass cls = schema.getClass(ODatabaseImport.EXPORT_IMPORT_CLASS_NAME);

    if (cls != null) {
      final int[] clusterIds = cls.getClusterIds();
      clusterDifference = clusterIds.length;
    }
  }

  @Override
  public void run() {
    compare();
  }

  public boolean compare() {
    try {
      ODocumentHelper.RIDMapper ridMapper = null;
      if (autoDetectExportImportMap) {
        listener.onMessage(
            "\n"
                + "Auto discovery of mapping between RIDs of exported and imported records is"
                + " switched on, try to discover mapping data on disk.");
        if (databaseTwo.getMetadata().getSchema().getClass(ODatabaseImport.EXPORT_IMPORT_CLASS_NAME)
            != null) {
          listener.onMessage("\nMapping data were found and will be loaded.");
          ridMapper =
              rid -> {
                if (rid == null) {
                  return null;
                }

                if (!rid.isPersistent()) {
                  return null;
                }

                databaseTwo.activateOnCurrentThread();
                try (final YTResultSet resultSet =
                    databaseTwo.query(
                        "select value from "
                            + ODatabaseImport.EXPORT_IMPORT_CLASS_NAME
                            + " where key = ?",
                        rid.toString())) {
                  if (resultSet.hasNext()) {
                    return new YTRecordId(resultSet.next().<String>getProperty("value"));
                  }
                  return null;
                }
              };
        } else {
          listener.onMessage("\nMapping data were not found.");
        }
      }

      compareClusters();
      compareRecords(ridMapper);

      compareSchema();
      compareIndexes(ridMapper);

      if (differences == 0) {
        listener.onMessage("\n\nDatabases match.");
        return true;
      } else {
        listener.onMessage("\n\nDatabases do not match. Found " + differences + " difference(s).");
        return false;
      }
    } catch (Exception e) {
      OLogManager.instance()
          .error(
              this,
              "Error on comparing database '%s' against '%s'",
              e,
              databaseOne.getName(),
              databaseTwo.getName());
      throw new ODatabaseExportException(
          "Error on comparing database '"
              + databaseOne.getName()
              + "' against '"
              + databaseTwo.getName()
              + "'",
          e);
    } finally {
      makeDbCall(
          databaseOne,
          (ODbRelatedCall<Void>)
              database -> {
                database.close();
                return null;
              });
      makeDbCall(
          databaseTwo,
          (ODbRelatedCall<Void>)
              database -> {
                database.close();
                return null;
              });
    }
  }

  private void compareSchema() {
    YTSchema schema1 = databaseOne.getMetadata().getImmutableSchemaSnapshot();
    YTSchema schema2 = databaseTwo.getMetadata().getImmutableSchemaSnapshot();
    boolean ok = true;
    for (YTClass clazz : schema1.getClasses()) {
      YTClass clazz2 = schema2.getClass(clazz.getName());

      if (clazz2 == null) {
        listener.onMessage("\n- ERR: Class definition " + clazz.getName() + " for DB2 is null.");
        continue;
      }

      final List<String> sc1 = clazz.getSuperClassesNames();
      final List<String> sc2 = clazz2.getSuperClassesNames();

      if (!sc1.isEmpty() || !sc2.isEmpty()) {
        if (!new HashSet<>(sc1).containsAll(sc2) || !new HashSet<>(sc2).containsAll(sc1)) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " in DB1 is not equals in superclasses in DB2.");
          ok = false;
        }
      }
      if (!clazz.getClassIndexes(databaseOne).equals(clazz2.getClassIndexes(databaseTwo))) {
        listener.onMessage(
            "\n- ERR: Class definition for "
                + clazz.getName()
                + " in DB1 is not equals in indexes in DB2.");
        ok = false;
      }
      if (!Arrays.equals(clazz.getClusterIds(), clazz2.getClusterIds())) {
        listener.onMessage(
            "\n- ERR: Class definition for "
                + clazz.getName()
                + " in DB1 is not equals in clusters in DB2.");
        ok = false;
      }
      if (!clazz.getCustomKeys().equals(clazz2.getCustomKeys())) {
        listener.onMessage(
            "\n- ERR: Class definition for "
                + clazz.getName()
                + " in DB1 is not equals in custom keys in DB2.");
        ok = false;
      }
      if (clazz.getOverSize() != clazz2.getOverSize()) {
        listener.onMessage(
            "\n- ERR: Class definition for "
                + clazz.getName()
                + " in DB1 is not equals in overSize in DB2.");
        ok = false;
      }

      if (clazz.getDefaultClusterId() != clazz2.getDefaultClusterId()) {
        listener.onMessage(
            "\n- ERR: Class definition for "
                + clazz.getName()
                + " in DB1 is not equals in default cluser id in DB2.");
        ok = false;
      }

      // if (clazz.getSize() != clazz2.getSize()) {
      // listener.onMessage("\n- ERR: Class definition for " + clazz.getName() + " in DB1 is not
      // equals in size in DB2.");
      // ok = false;
      // }

      for (YTProperty prop : clazz.declaredProperties()) {
        YTProperty prop2 = clazz2.getProperty(prop.getName());
        if (prop2 == null) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as missed property "
                  + prop.getName()
                  + "in DB2.");
          ok = false;
          continue;
        }
        if (prop.getType() != prop2.getType()) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as not same type for property "
                  + prop.getName()
                  + "in DB2. ");
          ok = false;
        }

        if (prop.getLinkedType() != prop2.getLinkedType()) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as not same linkedtype for property "
                  + prop.getName()
                  + "in DB2.");
          ok = false;
        }

        if (prop.getMin() != null) {
          if (!prop.getMin().equals(prop2.getMin())) {
            listener.onMessage(
                "\n- ERR: Class definition for "
                    + clazz.getName()
                    + " as not same min for property "
                    + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }
        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage(
                "\n- ERR: Class definition for "
                    + clazz.getName()
                    + " as not same max for property "
                    + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }

        if (prop.getMax() != null) {
          if (!prop.getMax().equals(prop2.getMax())) {
            listener.onMessage(
                "\n- ERR: Class definition for "
                    + clazz.getName()
                    + " as not same regexp for property "
                    + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getLinkedClass().equals(prop2.getLinkedClass())) {
            listener.onMessage(
                "\n- ERR: Class definition for "
                    + clazz.getName()
                    + " as not same linked class for property "
                    + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }

        if (prop.getLinkedClass() != null) {
          if (!prop.getCustomKeys().equals(prop2.getCustomKeys())) {
            listener.onMessage(
                "\n- ERR: Class definition for "
                    + clazz.getName()
                    + " as not same custom keys for property "
                    + prop.getName()
                    + "in DB2.");
            ok = false;
          }
        }
        if (prop.isMandatory() != prop2.isMandatory()) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as not same mandatory flag for property "
                  + prop.getName()
                  + "in DB2.");
          ok = false;
        }
        if (prop.isNotNull() != prop2.isNotNull()) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as not same nut null flag for property "
                  + prop.getName()
                  + "in DB2.");
          ok = false;
        }
        if (prop.isReadonly() != prop2.isReadonly()) {
          listener.onMessage(
              "\n- ERR: Class definition for "
                  + clazz.getName()
                  + " as not same readonly flag setting for property "
                  + prop.getName()
                  + "in DB2.");
          ok = false;
        }
      }
      if (!ok) {
        ++differences;
        ok = true;
      }
    }
  }

  @SuppressWarnings({"ObjectAllocationInLoop"})
  private void compareIndexes(ODocumentHelper.RIDMapper ridMapper) {
    listener.onMessage("\nStarting index comparison:");

    boolean ok = true;

    final OIndexManagerAbstract indexManagerOne =
        makeDbCall(databaseOne, database -> database.getMetadata().getIndexManagerInternal());

    final OIndexManagerAbstract indexManagerTwo =
        makeDbCall(databaseTwo, database -> database.getMetadata().getIndexManagerInternal());

    final Collection<? extends OIndex> indexesOne =
        makeDbCall(
            databaseOne,
            (ODbRelatedCall<Collection<? extends OIndex>>) indexManagerOne::getIndexes);

    int indexesSizeOne = makeDbCall(databaseTwo, database -> indexesOne.size());

    int indexesSizeTwo =
        makeDbCall(databaseTwo, database -> indexManagerTwo.getIndexes(database).size());

    if (makeDbCall(
        databaseTwo,
        database ->
            indexManagerTwo.getIndex(database, ODatabaseImport.EXPORT_IMPORT_INDEX_NAME) != null)) {
      indexesSizeTwo--;
    }

    if (indexesSizeOne != indexesSizeTwo) {
      ok = false;
      listener.onMessage("\n- ERR: Amount of indexes are different.");
      listener.onMessage("\n--- DB1: " + indexesSizeOne);
      listener.onMessage("\n--- DB2: " + indexesSizeTwo);
      listener.onMessage("\n");
      ++differences;
    }

    final Iterator<? extends OIndex> iteratorOne =
        makeDbCall(
            databaseOne,
            (ODbRelatedCall<Iterator<? extends OIndex>>) database -> indexesOne.iterator());

    while (makeDbCall(databaseOne, database -> iteratorOne.hasNext())) {
      final OIndex indexOne =
          makeDbCall(databaseOne, (ODbRelatedCall<OIndex>) database -> iteratorOne.next());

      @SuppressWarnings("ObjectAllocationInLoop") final String indexName = makeDbCall(databaseOne,
          database -> indexOne.getName());
      if (excludeIndexes.contains(indexName)) {
        continue;
      }

      @SuppressWarnings("ObjectAllocationInLoop") final OIndex indexTwo =
          makeDbCall(
              databaseTwo, database -> indexManagerTwo.getIndex(database, indexOne.getName()));

      if (indexTwo == null) {
        ok = false;
        listener.onMessage("\n- ERR: Index " + indexOne.getName() + " is absent in DB2.");
        ++differences;
        continue;
      }

      if (!indexOne.getType().equals(indexTwo.getType())) {
        ok = false;
        listener.onMessage(
            "\n- ERR: Index types for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOne.getType());
        listener.onMessage("\n--- DB2: " + indexTwo.getType());
        listener.onMessage("\n");
        ++differences;
        continue;
      }

      if (!indexOne.getClusters().equals(indexTwo.getClusters())) {
        ok = false;
        listener.onMessage(
            "\n- ERR: Clusters to index for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOne.getClusters());
        listener.onMessage("\n--- DB2: " + indexTwo.getClusters());
        listener.onMessage("\n");
        ++differences;
        continue;
      }

      if (indexOne.getDefinition() == null && indexTwo.getDefinition() != null) {
        // THIS IS NORMAL SINCE 3.0 DUE OF REMOVING OF INDEX WITHOUT THE DEFINITION,  THE IMPORTER
        // WILL CREATE THE DEFINITION
        listener.onMessage(
            "\n- WARN: Index definition for index " + indexOne.getName() + " for DB2 is not null.");
        continue;
      } else {
        if (indexOne.getDefinition() != null && indexTwo.getDefinition() == null) {
          ok = false;
          listener.onMessage(
              "\n- ERR: Index definition for index " + indexOne.getName() + " for DB2 is null.");
          ++differences;
          continue;
        } else {
          if (indexOne.getDefinition() != null
              && !indexOne.getDefinition().equals(indexTwo.getDefinition())) {
            ok = false;
            listener.onMessage(
                "\n- ERR: Index definitions for index " + indexOne.getName() + " are different.");
            listener.onMessage("\n--- DB1: " + indexOne.getDefinition());
            listener.onMessage("\n--- DB2: " + indexTwo.getDefinition());
            listener.onMessage("\n");
            ++differences;
            continue;
          }
        }
      }

      final long indexOneSize =
          makeDbCall(databaseOne, database -> ((OIndexInternal) indexOne).size(databaseOne));

      @SuppressWarnings("ObjectAllocationInLoop") final long indexTwoSize =
          makeDbCall(databaseTwo, database -> ((OIndexInternal) indexTwo).size(databaseTwo));

      if (indexOneSize != indexTwoSize) {
        ok = false;
        listener.onMessage(
            "\n- ERR: Amount of entries for index " + indexOne.getName() + " are different.");
        listener.onMessage("\n--- DB1: " + indexOneSize);
        listener.onMessage("\n--- DB2: " + indexTwoSize);
        listener.onMessage("\n");
        ++differences;
      }

      if (compareIndexMetadata) {
        final var metadataOne = indexOne.getMetadata();
        final var metadataTwo = indexTwo.getMetadata();

        if (metadataOne == null && metadataTwo != null) {
          ok = false;
          listener.onMessage(
              "\n- ERR: Metadata for index "
                  + indexOne.getName()
                  + " for DB1 is null but for DB2 is not.");
          listener.onMessage("\n");
          ++differences;
        } else {
          if (metadataOne != null && metadataTwo == null) {
            ok = false;
            listener.onMessage(
                "\n- ERR: Metadata for index "
                    + indexOne.getName()
                    + " for DB1 is not null but for DB2 is null.");
            listener.onMessage("\n");
            ++differences;
          } else {
            if (!Objects.equals(metadataOne, metadataTwo)) {
              ok = false;
              listener.onMessage(
                  "\n- ERR: Metadata for index "
                      + indexOne.getName()
                      + " for DB1 and for DB2 are different.");
              makeDbCall(
                  databaseOne,
                  database -> {
                    listener.onMessage("\n--- M1: " + metadataOne);
                    return null;
                  });
              makeDbCall(
                  databaseTwo,
                  database -> {
                    listener.onMessage("\n--- M2: " + metadataTwo);
                    return null;
                  });
              listener.onMessage("\n");
              ++differences;
            }
          }
        }
      }

      if (((compareEntriesForAutomaticIndexes && !indexOne.getType().equals("DICTIONARY"))
          || !indexOne.isAutomatic())) {

        try (final Stream<Object> keyStream =
            makeDbCall(databaseOne, database -> ((OIndexInternal) indexOne).keyStream())) {
          final Iterator<Object> indexKeyIteratorOne =
              makeDbCall(databaseOne, database -> keyStream.iterator());
          while (makeDbCall(databaseOne, database -> indexKeyIteratorOne.hasNext())) {
            final Object indexKey = makeDbCall(databaseOne, database -> indexKeyIteratorOne.next());

            try (Stream<YTRID> indexOneStream =
                makeDbCall(databaseOne,
                    database -> indexOne.getInternal().getRids(database, indexKey))) {
              try (Stream<YTRID> indexTwoValue =
                  makeDbCall(databaseTwo,
                      database -> indexTwo.getInternal().getRids(database, indexKey))) {
                differences +=
                    compareIndexStreams(
                        indexKey, indexOneStream, indexTwoValue, ridMapper, listener);
              }
            }
            ok = ok && differences > 0;
          }
        }
      }
    }

    if (ok) {
      listener.onMessage("OK");
    }
  }

  private static int compareIndexStreams(
      final Object indexKey,
      final Stream<YTRID> streamOne,
      final Stream<YTRID> streamTwo,
      final ODocumentHelper.RIDMapper ridMapper,
      final OCommandOutputListener listener) {
    final Set<YTRID> streamTwoSet = new HashSet<>();

    final Iterator<YTRID> streamOneIterator = streamOne.iterator();
    final Iterator<YTRID> streamTwoIterator = streamTwo.iterator();

    int differences = 0;
    while (streamOneIterator.hasNext()) {
      YTRID rid;
      if (ridMapper == null) {
        rid = streamOneIterator.next();
      } else {
        final YTRID streamOneRid = streamOneIterator.next();
        rid = ridMapper.map(streamOneRid);
        if (rid == null) {
          rid = streamOneRid;
        }
      }

      if (!streamTwoSet.remove(rid)) {
        if (!streamTwoIterator.hasNext()) {
          listener.onMessage(
              "\r\nEntry " + indexKey + ":" + rid + " is present in DB1 but absent in DB2");
          differences++;
        } else {
          boolean found = false;
          while (streamTwoIterator.hasNext()) {
            final YTRID streamRid = streamTwoIterator.next();
            if (streamRid.equals(rid)) {
              found = true;
              break;
            }

            streamTwoSet.add(streamRid);
          }

          if (!found) {
            listener.onMessage(
                "\r\nEntry " + indexKey + ":" + rid + " is present in DB1 but absent in DB2");
          }
        }
      }
    }

    while (streamTwoIterator.hasNext()) {
      final YTRID rid = streamTwoIterator.next();
      listener.onMessage(
          "\r\nEntry " + indexKey + ":" + rid + " is present in DB2 but absent in DB1");

      differences++;
    }

    for (final YTRID rid : streamTwoSet) {
      listener.onMessage(
          "\r\nEntry " + indexKey + ":" + rid + " is present in DB2 but absent in DB1");

      differences++;
    }
    return differences;
  }

  @SuppressWarnings("ObjectAllocationInLoop")
  private void compareClusters() {
    listener.onMessage("\nStarting shallow comparison of clusters:");

    listener.onMessage("\nChecking the number of clusters...");

    Collection<String> clusterNames1 =
        makeDbCall(databaseOne, YTDatabaseSessionInternal::getClusterNames);

    Collection<String> clusterNames2 =
        makeDbCall(databaseTwo, YTDatabaseSessionInternal::getClusterNames);

    if (clusterNames1.size() != clusterNames2.size() - clusterDifference) {
      listener.onMessage(
          "ERR: cluster sizes are different: "
              + clusterNames1.size()
              + " <-> "
              + clusterNames2.size());
      ++differences;
    }

    boolean ok;

    for (final String clusterName : clusterNames1) {
      // CHECK IF THE CLUSTER IS INCLUDED
      ok = true;
      final int cluster1Id =
          makeDbCall(databaseTwo, database -> database.getClusterIdByName(clusterName));

      listener.onMessage(
          "\n- Checking cluster " + String.format("%-25s: ", "'" + clusterName + "'"));

      if (cluster1Id == -1) {
        listener.onMessage(
            "ERR: cluster name '"
                + clusterName
                + "' was not found on database "
                + databaseTwo.getName());
        ++differences;
        ok = false;
      }

      final int cluster2Id =
          makeDbCall(databaseOne, database -> database.getClusterIdByName(clusterName));
      if (cluster1Id != cluster2Id) {
        listener.onMessage(
            "ERR: cluster id is different for cluster "
                + clusterName
                + ": "
                + cluster2Id
                + " <-> "
                + cluster1Id);
        ++differences;
        ok = false;
      }

      long countCluster1 =
          makeDbCall(databaseOne, database -> database.countClusterElements(cluster1Id));
      long countCluster2 =
          makeDbCall(databaseOne, database -> database.countClusterElements(cluster2Id));

      if (countCluster1 != countCluster2) {
        listener.onMessage(
            "ERR: number of records different in cluster '"
                + clusterName
                + "' (id="
                + cluster1Id
                + "): "
                + countCluster1
                + " <-> "
                + countCluster2);
        ++differences;
        ok = false;
      }

      if (ok) {
        listener.onMessage("OK");
      }
    }

    listener.onMessage("\n\nShallow analysis done.");
  }

  @SuppressWarnings("ObjectAllocationInLoop")
  private void compareRecords(ODocumentHelper.RIDMapper ridMapper) {
    listener.onMessage(
        "\nStarting deep comparison record by record. This may take a few minutes. Wait please...");

    Collection<String> clusterNames1 =
        makeDbCall(databaseOne, YTDatabaseSessionInternal::getClusterNames);

    for (final String clusterName : clusterNames1) {
      // CHECK IF THE CLUSTER IS INCLUDED
      final int clusterId1 =
          makeDbCall(databaseOne, database -> database.getClusterIdByName(clusterName));

      @SuppressWarnings("ObjectAllocationInLoop") final YTRecordId rid1 = new YTRecordId(
          clusterId1);

      YTDatabaseSessionInternal selectedDatabase = databaseOne;

      OPhysicalPosition[] physicalPositions =
          makeDbCall(
              selectedDatabase,
              database ->
                  database
                      .getStorage()
                      .ceilingPhysicalPositions(databaseOne, clusterId1, new OPhysicalPosition(0)));

      OStorageConfiguration configuration1 =
          makeDbCall(databaseOne, database -> database.getStorageInfo().getConfiguration());
      OStorageConfiguration configuration2 =
          makeDbCall(databaseTwo, database -> database.getStorageInfo().getConfiguration());

      String storageType1 = makeDbCall(databaseOne, database -> database.getStorage().getType());
      String storageType2 = makeDbCall(databaseTwo, database -> database.getStorage().getType());

      long recordsCounter = 0;
      while (physicalPositions.length > 0) {
        for (OPhysicalPosition physicalPosition : physicalPositions) {
          try {
            recordsCounter++;

            databaseOne.activateOnCurrentThread();
            @SuppressWarnings("ObjectAllocationInLoop") final YTEntityImpl doc1 = new YTEntityImpl();
            databaseTwo.activateOnCurrentThread();

            @SuppressWarnings("ObjectAllocationInLoop") final YTEntityImpl doc2 = new YTEntityImpl();

            final long position = physicalPosition.clusterPosition;
            rid1.setClusterPosition(position);

            final YTRecordId rid2;
            if (ridMapper == null) {
              rid2 = rid1;
            } else {
              final YTRID newRid = ridMapper.map(rid1);
              if (newRid == null) {
                rid2 = rid1;
              } else
              //noinspection ObjectAllocationInLoop
              {
                rid2 = new YTRecordId(newRid);
              }
            }

            if (skipRecord(
                rid1, rid2, configuration1, configuration2, storageType1, storageType2)) {
              continue;
            }

            final ORawBuffer buffer1 =
                makeDbCall(
                    databaseOne,
                    database -> database.getStorage()
                        .readRecord(databaseOne, rid1, true, false, null));
            final ORawBuffer buffer2 =
                makeDbCall(
                    databaseTwo,
                    database -> database.getStorage()
                        .readRecord(databaseTwo, rid2, true, false, null));

            //noinspection StatementWithEmptyBody
            if (buffer1 == null && buffer2 == null) {
              // BOTH RECORD NULL, OK
            } else {
              if (buffer1 == null) {
                // REC1 NULL
                listener.onMessage(
                    "\n- ERR: RID=" + clusterId1 + ":" + position + " is null in DB1");
                ++differences;
              } else {
                if (buffer2 == null) {
                  // REC2 NULL
                  listener.onMessage(
                      "\n- ERR: RID=" + clusterId1 + ":" + position + " is null in DB2");
                  ++differences;
                } else {
                  if (buffer1.recordType != buffer2.recordType) {
                    listener.onMessage(
                        "\n- ERR: RID="
                            + clusterId1
                            + ":"
                            + position
                            + " recordType is different: "
                            + (char) buffer1.recordType
                            + " <-> "
                            + (char) buffer2.recordType);
                    ++differences;
                  }

                  //noinspection StatementWithEmptyBody
                  if (buffer1.buffer == null && buffer2.buffer == null) {
                    // Both null so both equals
                  } else {
                    if (buffer1.buffer == null) {
                      listener.onMessage(
                          "\n- ERR: RID="
                              + clusterId1
                              + ":"
                              + position
                              + " content is different: null <-> "
                              + buffer2.buffer.length);
                      ++differences;

                    } else {
                      if (buffer2.buffer == null) {
                        listener.onMessage(
                            "\n- ERR: RID="
                                + clusterId1
                                + ":"
                                + position
                                + " content is different: "
                                + buffer1.buffer.length
                                + " <-> null");
                        ++differences;

                      } else {
                        if (buffer1.recordType == YTEntityImpl.RECORD_TYPE) {
                          // DOCUMENT: TRY TO INSTANTIATE AND COMPARE

                          makeDbCall(
                              databaseOne,
                              database -> {
                                ORecordInternal.unsetDirty(doc1);
                                ORecordInternal.fromStream(doc1, buffer1.buffer, database);
                                return null;
                              });

                          makeDbCall(
                              databaseTwo,
                              database -> {
                                ORecordInternal.unsetDirty(doc2);
                                ORecordInternal.fromStream(doc2, buffer2.buffer, database);
                                return null;
                              });

                          if (rid1.toString().equals(configuration1.getSchemaRecordId())
                              && rid1.toString().equals(configuration2.getSchemaRecordId())) {
                            makeDbCall(
                                databaseOne,
                                database -> {
                                  convertSchemaDoc(doc1);
                                  return null;
                                });

                            makeDbCall(
                                databaseTwo,
                                database -> {
                                  convertSchemaDoc(doc2);
                                  return null;
                                });
                          }

                          if (!ODocumentHelper.hasSameContentOf(
                              doc1, databaseOne, doc2, databaseTwo, ridMapper)) {
                            listener.onMessage(
                                "\n- ERR: RID="
                                    + clusterId1
                                    + ":"
                                    + position
                                    + " document content is different");
                            //noinspection ObjectAllocationInLoop
                            listener.onMessage("\n--- REC1: " + new String(buffer1.buffer));
                            //noinspection ObjectAllocationInLoop
                            listener.onMessage("\n--- REC2: " + new String(buffer2.buffer));
                            listener.onMessage("\n");
                            ++differences;
                          }
                        } else {
                          if (buffer1.buffer.length != buffer2.buffer.length) {
                            // CHECK IF THE TRIMMED SIZE IS THE SAME
                            @SuppressWarnings("ObjectAllocationInLoop") final String rec1 = new String(
                                buffer1.buffer).trim();
                            @SuppressWarnings("ObjectAllocationInLoop") final String rec2 = new String(
                                buffer2.buffer).trim();

                            if (rec1.length() != rec2.length()) {
                              listener.onMessage(
                                  "\n- ERR: RID="
                                      + clusterId1
                                      + ":"
                                      + position
                                      + " content length is different: "
                                      + buffer1.buffer.length
                                      + " <-> "
                                      + buffer2.buffer.length);

                              if (buffer1.recordType == YTEntityImpl.RECORD_TYPE) {
                                listener.onMessage("\n--- REC1: " + rec1);
                              }
                              if (buffer2.recordType == YTEntityImpl.RECORD_TYPE) {
                                listener.onMessage("\n--- REC2: " + rec2);
                              }
                              listener.onMessage("\n");

                              ++differences;
                            }
                          } else {
                            // CHECK BYTE PER BYTE
                            for (int b = 0; b < buffer1.buffer.length; ++b) {
                              if (buffer1.buffer[b] != buffer2.buffer[b]) {
                                listener.onMessage(
                                    "\n- ERR: RID="
                                        + clusterId1
                                        + ":"
                                        + position
                                        + " content is different at byte #"
                                        + b
                                        + ": "
                                        + buffer1.buffer[b]
                                        + " <-> "
                                        + buffer2.buffer[b]);
                                listener.onMessage("\n--- REC1: " + new String(buffer1.buffer));
                                listener.onMessage("\n--- REC2: " + new String(buffer2.buffer));
                                listener.onMessage("\n");
                                ++differences;
                                break;
                              }
                            }
                          }
                        }
                      }
                    }
                  }
                }
              }
            }
          } catch (RuntimeException e) {
            OLogManager.instance()
                .error(this, "Error during data comparison of records with rid " + rid1, e);
            throw e;
          }
        }
        final OPhysicalPosition[] curPosition = physicalPositions;
        physicalPositions =
            makeDbCall(
                selectedDatabase,
                database ->
                    database
                        .getStorage()
                        .higherPhysicalPositions(databaseOne, clusterId1,
                            curPosition[curPosition.length - 1]));
        if (recordsCounter % 10000 == 0) {
          listener.onMessage(
              "\n"
                  + recordsCounter
                  + " records were processed for cluster "
                  + clusterName
                  + " ...");
        }
      }

      listener.onMessage(
          "\nCluster comparison was finished, "
              + recordsCounter
              + " records were processed for cluster "
              + clusterName
              + " ...");
    }
  }

  private static boolean skipRecord(
      YTRecordId rid1,
      YTRecordId rid2,
      OStorageConfiguration configuration1,
      OStorageConfiguration configuration2,
      String storageType1,
      String storageType2) {
    if (rid1.equals(new YTRecordId(configuration1.getIndexMgrRecordId()))
        || rid2.equals(new YTRecordId(configuration2.getIndexMgrRecordId()))) {
      return true;
    }
    if (rid1.equals(new YTRecordId(configuration1.getSchemaRecordId()))
        || rid2.equals(new YTRecordId(configuration2.getSchemaRecordId()))) {
      return true;
    }
    if ((rid1.getClusterId() == 0 && rid1.getClusterPosition() == 0)
        || (rid2.getClusterId() == 0 && rid2.getClusterPosition() == 0)) {
      // Skip the compare of raw structure if the storage type are different, due the fact
      // that are different by definition.
      return !storageType1.equals(storageType2);
    }
    return false;
  }

  public void setCompareIndexMetadata(boolean compareIndexMetadata) {
    this.compareIndexMetadata = compareIndexMetadata;
  }

  public void setCompareEntriesForAutomaticIndexes(boolean compareEntriesForAutomaticIndexes) {
    this.compareEntriesForAutomaticIndexes = compareEntriesForAutomaticIndexes;
  }

  public void setAutoDetectExportImportMap(boolean autoDetectExportImportMap) {
    this.autoDetectExportImportMap = autoDetectExportImportMap;
  }

  private static void convertSchemaDoc(final YTEntityImpl document) {
    if (document.field("classes") != null) {
      document.setFieldType("classes", YTType.EMBEDDEDSET);
      for (YTEntityImpl classDoc : document.<Set<YTEntityImpl>>field("classes")) {
        classDoc.setFieldType("properties", YTType.EMBEDDEDSET);
      }
    }
  }
}
