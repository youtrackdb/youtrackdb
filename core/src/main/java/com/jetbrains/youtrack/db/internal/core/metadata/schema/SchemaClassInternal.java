package com.jetbrains.youtrack.db.internal.core.metadata.schema;

import com.jetbrains.youtrack.db.api.DatabaseSession;
import com.jetbrains.youtrack.db.api.schema.PropertyType;
import com.jetbrains.youtrack.db.api.schema.SchemaClass;
import com.jetbrains.youtrack.db.api.schema.SchemaProperty;
import com.jetbrains.youtrack.db.internal.core.db.DatabaseSessionInternal;
import com.jetbrains.youtrack.db.internal.core.index.Index;
import com.jetbrains.youtrack.db.internal.core.record.impl.EntityImpl;
import java.util.Collection;
import java.util.Set;

public interface SchemaClassInternal extends SchemaClass {

  SchemaClass truncateCluster(DatabaseSession session, String clusterName);

  ClusterSelectionStrategy getClusterSelection(DatabaseSession db);

  SchemaClass setClusterSelection(DatabaseSession session,
      final ClusterSelectionStrategy clusterSelection);

  int getClusterForNewInstance(DatabaseSession db, final EntityImpl entity);

  Set<Index> getInvolvedIndexesInternal(DatabaseSession session, String... fields);

  Set<Index> getInvolvedIndexesInternal(DatabaseSession session, final Collection<String> fields);

  SchemaProperty createProperty(
      DatabaseSession session, final String iPropertyName,
      final PropertyType iType,
      final PropertyType iLinkedType,
      final boolean unsafe);

  SchemaProperty createProperty(
      DatabaseSession session, final String iPropertyName,
      final PropertyType iType,
      final SchemaClass iLinkedClass,
      final boolean unsafe);

  Set<Index> getIndexesInternal(DatabaseSession session);

  void getIndexesInternal(DatabaseSession session, Collection<Index> indices);

  long count(DatabaseSession session);

  void truncate(DatabaseSession session);

  long count(DatabaseSession session, final boolean isPolymorphic);

  SchemaPropertyInternal getPropertyInternal(DatabaseSessionInternal db, String propertyName);

  Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session, String... fields);

  Set<Index> getClassInvolvedIndexesInternal(DatabaseSession session,
      final Collection<String> fields);

  Set<Index> getClassIndexesInternal(DatabaseSession session);

  Index getClassIndex(DatabaseSession session, final String name);

  SchemaClass set(DatabaseSession session, final ATTRIBUTES attribute, final Object value);


  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>All indexes sorted by their count of parameters in ascending order. If there are indexes
   * for the given set of fields in super class they will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   */
  Set<String> getInvolvedIndexes(DatabaseSession session, Collection<String> fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>All indexes sorted by their count of parameters in ascending order. If there are indexes
   * for the given set of fields in super class they will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see #getInvolvedIndexes(DatabaseSession, Collection)
   */
  Set<String> getInvolvedIndexes(DatabaseSession session, String... fields);

  /**
   * Returns list of indexes that contain passed in fields names as their first keys. Order of
   * fields does not matter.
   *
   * <p>Indexes that related only to the given class will be returned.
   *
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   */
  Set<String> getClassInvolvedIndexes(DatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return list of indexes that contain passed in fields names as their first keys.
   * @see #getClassInvolvedIndexes(DatabaseSession, Collection)
   */
  Set<String> getClassInvolvedIndexes(DatabaseSession session, String... fields);

  /**
   * Indicates whether given fields are contained as first key fields in class indexes. Order of
   * fields does not matter. If there are indexes for the given set of fields in super class they
   * will be taken into account.
   *
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   */
  boolean areIndexed(DatabaseSession session, Collection<String> fields);

  /**
   * @param session
   * @param fields  Field names.
   * @return <code>true</code> if given fields are contained as first key fields in class indexes.
   * @see #areIndexed(DatabaseSession, Collection)
   */
  boolean areIndexed(DatabaseSession session, String... fields);

  /**
   * @return All indexes for given class, not the inherited ones.
   */
  Set<String> getClassIndexes(DatabaseSession session);

  /**
   * @return All indexes for given class and its super classes.
   */
  Set<String> getIndexes(DatabaseSession session);
}
