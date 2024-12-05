package com.orientechnologies.core.record.impl;

import com.orientechnologies.common.log.OLogManager;
import com.orientechnologies.common.util.OPair;
import com.orientechnologies.core.db.ODatabaseRecordThreadLocal;
import com.orientechnologies.core.db.YTDatabaseSession;
import com.orientechnologies.core.db.YTDatabaseSessionInternal;
import com.orientechnologies.core.db.record.LinkList;
import com.orientechnologies.core.db.record.ORecordOperation;
import com.orientechnologies.core.db.record.YTIdentifiable;
import com.orientechnologies.core.db.record.ridbag.RidBag;
import com.orientechnologies.core.exception.YTDatabaseException;
import com.orientechnologies.core.exception.YTRecordNotFoundException;
import com.orientechnologies.core.id.ChangeableRecordId;
import com.orientechnologies.core.id.YTRID;
import com.orientechnologies.core.metadata.schema.YTClass;
import com.orientechnologies.core.metadata.schema.YTProperty;
import com.orientechnologies.core.metadata.schema.YTSchema;
import com.orientechnologies.core.metadata.schema.YTType;
import com.orientechnologies.core.record.ODirection;
import com.orientechnologies.core.record.ORecordInternal;
import com.orientechnologies.core.record.YTEdge;
import com.orientechnologies.core.record.YTRecord;
import com.orientechnologies.core.record.YTVertex;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.commons.collections4.IterableUtils;

public interface YTVertexInternal extends YTVertex, YTEntityInternal {

  @Nonnull
  YTEntityImpl getBaseDocument();

  @Override
  default Set<String> getPropertyNames() {
    return filterPropertyNames(getBaseDocument().getPropertyNamesInternal());
  }

  @Override
  default Set<String> getPropertyNamesInternal() {
    return getBaseDocument().getPropertyNamesInternal();
  }

  static Set<String> filterPropertyNames(Set<String> propertyNames) {
    var propertiesToRemove = new ArrayList<String>();

    for (var propertyName : propertyNames) {
      if (propertyName.startsWith(DIRECTION_IN_PREFIX)
          || propertyName.startsWith(DIRECTION_OUT_PREFIX)) {
        propertiesToRemove.add(propertyName);
      }
    }

    if (propertiesToRemove.isEmpty()) {
      return propertyNames;
    }

    for (var propertyToRemove : propertiesToRemove) {
      propertyNames.remove(propertyToRemove);
    }

    return propertyNames;
  }

  @Override
  default <RET> RET getProperty(String name) {
    checkPropertyName(name);

    return getBaseDocument().getPropertyInternal(name);
  }

  @Override
  default <RET> RET getPropertyInternal(String name, boolean lazyLoading) {
    return getBaseDocument().getPropertyInternal(name, lazyLoading);
  }

  @Override
  default <RET> RET getPropertyInternal(String name) {
    return getBaseDocument().getPropertyInternal(name);
  }

  @Override
  default <RET> RET getPropertyOnLoadValue(String name) {
    return getBaseDocument().getPropertyOnLoadValue(name);
  }

  @Nullable
  @Override
  default YTIdentifiable getLinkPropertyInternal(String name) {
    return getBaseDocument().getLinkPropertyInternal(name);
  }

  @Nullable
  @Override
  default YTIdentifiable getLinkProperty(String name) {
    checkPropertyName(name);

    return getBaseDocument().getLinkProperty(name);
  }

  static void checkPropertyName(String name) {
    if (name.startsWith(DIRECTION_OUT_PREFIX) || name.startsWith(DIRECTION_IN_PREFIX)) {
      throw new IllegalArgumentException(
          "Property name " + name + " is booked as a name that can be used to manage edges.");
    }
  }

  @Override
  default void setProperty(String name, Object value) {
    checkPropertyName(name);

    getBaseDocument().setPropertyInternal(name, value);
  }

  @Override
  default void setPropertyInternal(String name, Object value) {
    getBaseDocument().setPropertyInternal(name, value);
  }

  @Override
  default boolean hasProperty(final String propertyName) {
    checkPropertyName(propertyName);

    return getBaseDocument().hasProperty(propertyName);
  }

  @Override
  default void setProperty(String name, Object value, YTType... fieldType) {
    checkPropertyName(name);

    getBaseDocument().setPropertyInternal(name, value, fieldType);
  }

  @Override
  default void setPropertyInternal(String name, Object value, YTType... type) {
    getBaseDocument().setPropertyInternal(name, value, type);
  }

  @Override
  default <RET> RET removeProperty(String name) {
    checkPropertyName(name);

    return getBaseDocument().removePropertyInternal(name);
  }

  @Override
  default <RET> RET removePropertyInternal(String name) {
    return getBaseDocument().removePropertyInternal(name);
  }

  @Override
  default Iterable<YTVertex> getVertices(ODirection direction) {
    return getVertices(direction, (String[]) null);
  }

  @Override
  default Set<String> getEdgeNames() {
    return getEdgeNames(ODirection.BOTH);
  }

  @Override
  default Set<String> getEdgeNames(ODirection direction) {
    var propertyNames = getBaseDocument().getPropertyNamesInternal();
    var edgeNames = new HashSet<String>();

    for (var propertyName : propertyNames) {
      if (isConnectionToEdge(direction, propertyName)) {
        edgeNames.add(propertyName);
      }
    }

    return edgeNames;
  }

  static boolean isConnectionToEdge(ODirection direction, String propertyName) {
    return switch (direction) {
      case OUT -> propertyName.startsWith(DIRECTION_OUT_PREFIX);
      case IN -> propertyName.startsWith(DIRECTION_IN_PREFIX);
      case BOTH -> propertyName.startsWith(DIRECTION_OUT_PREFIX)
          || propertyName.startsWith(DIRECTION_IN_PREFIX);
    };
  }

  @Override
  default Iterable<YTVertex> getVertices(ODirection direction, String... type) {
    if (direction == ODirection.BOTH) {
      return IterableUtils.chainedIterable(
          getVertices(ODirection.OUT, type), getVertices(ODirection.IN, type));
    } else {
      Iterable<YTEdge> edges = getEdgesInternal(direction, type);
      return new OEdgeToVertexIterable(edges, direction);
    }
  }

  @Override
  default Iterable<YTVertex> getVertices(ODirection direction, YTClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (YTClass t : type) {
        types.add(t.getName());
      }
    }

    return getVertices(direction, types.toArray(new String[]{}));
  }

  @Override
  default YTEdge addEdge(YTVertex to) {
    return addEdge(to, YTEdgeInternal.CLASS_NAME);
  }

  @Override
  default YTEdge addLightWeightEdge(YTVertex to) {
    return addLightWeightEdge(to, YTEdgeInternal.CLASS_NAME);
  }

  @Override
  default YTEdge addEdge(YTVertex to, String type) {
    var db = getBaseDocument().getSession();
    return db.newEdge(this, to, type == null ? YTEdgeInternal.CLASS_NAME : type);
  }

  @Override
  default YTEdge addLightWeightEdge(YTVertex to, String label) {
    var db = getBaseDocument().getSession();
    return db.addLightweightEdge(this, to, label);
  }

  @Override
  default YTEdge addEdge(YTVertex to, YTClass type) {
    final String className;
    if (type != null) {
      className = type.getName();
    } else {
      className = YTEdgeInternal.CLASS_NAME;
    }

    return addEdge(to, className);
  }

  @Override
  default YTEdge addLightWeightEdge(YTVertex to, YTClass label) {
    final String className;

    if (label != null) {
      className = label.getName();
    } else {
      className = YTEdgeInternal.CLASS_NAME;
    }

    return addLightWeightEdge(to, className);
  }

  @Override
  default Iterable<YTEdge> getEdges(ODirection direction, YTClass... type) {
    List<String> types = new ArrayList<>();
    if (type != null) {
      for (YTClass t : type) {
        types.add(t.getName());
      }
    }
    return getEdges(direction, types.toArray(new String[]{}));
  }

  @Override
  default boolean isUnloaded() {
    return getBaseDocument().isUnloaded();
  }

  default boolean isNotBound(YTDatabaseSession session) {
    return getBaseDocument().isNotBound(session);
  }

  @Override
  default Iterable<YTEdge> getEdges(ODirection direction) {
    var prefixes =
        switch (direction) {
          case IN -> new String[]{DIRECTION_IN_PREFIX};
          case OUT -> new String[]{DIRECTION_OUT_PREFIX};
          case BOTH -> new String[]{DIRECTION_IN_PREFIX, DIRECTION_OUT_PREFIX};
        };

    Set<String> candidateClasses = new HashSet<>();

    var doc = getBaseDocument();
    for (var prefix : prefixes) {
      for (String fieldName : doc.calculatePropertyNames()) {
        if (fieldName.startsWith(prefix)) {
          if (fieldName.equals(prefix)) {
            candidateClasses.add(YTEdgeInternal.CLASS_NAME);
          } else {
            candidateClasses.add(fieldName.substring(prefix.length()));
          }
        }
      }
    }

    return getEdges(direction, candidateClasses.toArray(new String[]{}));
  }

  @Override
  default boolean exists() {
    return getBaseDocument().exists();
  }

  @Override
  default Iterable<YTEdge> getEdges(ODirection direction, String... labels) {
    return getEdgesInternal(direction, labels);
  }

  private Iterable<YTEdge> getEdgesInternal(ODirection direction, String[] labels) {
    var db = getBaseDocument().getSession();
    var schema = db.getMetadata().getImmutableSchemaSnapshot();

    labels = resolveAliases(schema, labels);
    Collection<String> fieldNames = null;
    var doc = getBaseDocument();
    if (labels != null && labels.length > 0) {
      // EDGE LABELS: CREATE FIELD NAME TABLE (FASTER THAN EXTRACT FIELD NAMES FROM THE DOCUMENT)
      var toLoadFieldNames = getEdgeFieldNames(schema, direction, labels);

      if (toLoadFieldNames != null) {
        // EARLY FETCH ALL THE FIELDS THAT MATTERS
        doc.deserializeFields(toLoadFieldNames.toArray(new String[]{}));
        fieldNames = toLoadFieldNames;
      }
    }

    if (fieldNames == null) {
      fieldNames = doc.calculatePropertyNames();
    }

    var iterables = new ArrayList<Iterable<YTEdge>>(fieldNames.size());
    for (var fieldName : fieldNames) {
      final OPair<ODirection, String> connection =
          getConnection(schema, direction, fieldName, labels);
      if (connection == null)
      // SKIP THIS FIELD
      {
        continue;
      }

      Object fieldValue;

      fieldValue = doc.getPropertyInternal(fieldName);

      if (fieldValue != null) {
        if (fieldValue instanceof YTIdentifiable) {
          var coll = Collections.singleton(fieldValue);
          iterables.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, 1));
        } else if (fieldValue instanceof Collection<?> coll) {
          // CREATE LAZY Iterable AGAINST COLLECTION FIELD
          iterables.add(new OEdgeIterator(this, coll, coll.iterator(), connection, labels, -1));
        } else if (fieldValue instanceof RidBag) {
          iterables.add(
              new OEdgeIterator(
                  this,
                  fieldValue,
                  ((RidBag) fieldValue).iterator(),
                  connection,
                  labels,
                  ((RidBag) fieldValue).size()));
        }
      }
    }

    if (iterables.size() == 1) {
      return iterables.get(0);
    } else if (iterables.isEmpty()) {
      return Collections.emptyList();
    }

    //noinspection unchecked
    return IterableUtils.chainedIterable(iterables.toArray(new Iterable[0]));
  }

  private static ArrayList<String> getEdgeFieldNames(
      YTSchema schema, final ODirection iDirection, String... classNames) {
    if (classNames == null)
    // FALL BACK TO LOAD ALL FIELD NAMES
    {
      return null;
    }

    if (classNames.length == 1 && classNames[0].equalsIgnoreCase(YTEdgeInternal.CLASS_NAME))
    // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
    {
      return null;
    }

    Set<String> allClassNames = new HashSet<>();
    for (String className : classNames) {
      allClassNames.add(className);
      YTClass clazz = schema.getClass(className);
      if (clazz != null) {
        allClassNames.add(clazz.getName()); // needed for aliases
        Collection<YTClass> subClasses = clazz.getAllSubclasses();
        for (YTClass subClass : subClasses) {
          allClassNames.add(subClass.getName());
        }
      }
    }

    var result = new ArrayList<String>(2 * allClassNames.size());
    for (String className : allClassNames) {
      switch (iDirection) {
        case OUT:
          result.add(DIRECTION_OUT_PREFIX + className);
          break;
        case IN:
          result.add(DIRECTION_IN_PREFIX + className);
          break;
        case BOTH:
          result.add(DIRECTION_OUT_PREFIX + className);
          result.add(DIRECTION_IN_PREFIX + className);
          break;
      }
    }

    return result;
  }

  static OPair<ODirection, String> getConnection(
      final YTSchema schema,
      final ODirection direction,
      final String fieldName,
      String... classNames) {
    if (classNames != null
        && classNames.length == 1
        && classNames[0].equalsIgnoreCase(YTEdgeInternal.CLASS_NAME))
    // DEFAULT CLASS, TREAT IT AS NO CLASS/LABEL
    {
      classNames = null;
    }

    if (direction == ODirection.OUT || direction == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "out_"
      if (fieldName.startsWith(DIRECTION_OUT_PREFIX)) {
        if (classNames == null || classNames.length == 0) {
          return new OPair<>(ODirection.OUT, getConnectionClass(ODirection.OUT, fieldName));
        }

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : classNames) {
          if (fieldName.equals(DIRECTION_OUT_PREFIX + clsName)) {
            return new OPair<>(ODirection.OUT, clsName);
          }

          // GO DOWN THROUGH THE INHERITANCE TREE
          YTClass type = schema.getClass(clsName);
          if (type != null) {
            for (YTClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();

              if (fieldName.equals(DIRECTION_OUT_PREFIX + clsName)) {
                return new OPair<>(ODirection.OUT, clsName);
              }
            }
          }
        }
      }
    }

    if (direction == ODirection.IN || direction == ODirection.BOTH) {
      // FIELDS THAT STARTS WITH "in_"
      if (fieldName.startsWith(DIRECTION_IN_PREFIX)) {
        if (classNames == null || classNames.length == 0) {
          return new OPair<>(ODirection.IN, getConnectionClass(ODirection.IN, fieldName));
        }

        // CHECK AGAINST ALL THE CLASS NAMES
        for (String clsName : classNames) {

          if (fieldName.equals(DIRECTION_IN_PREFIX + clsName)) {
            return new OPair<>(ODirection.IN, clsName);
          }

          // GO DOWN THROUGH THE INHERITANCE TREE
          YTClass type = schema.getClass(clsName);
          if (type != null) {
            for (YTClass subType : type.getAllSubclasses()) {
              clsName = subType.getName();
              if (fieldName.equals(DIRECTION_IN_PREFIX + clsName)) {
                return new OPair<>(ODirection.IN, clsName);
              }
            }
          }
        }
      }
    }

    // NOT FOUND
    return null;
  }

  private static void replaceLinks(
      final YTEntityImpl vertex,
      final String fieldName,
      final YTIdentifiable iVertexToRemove,
      final YTIdentifiable newVertex) {
    if (vertex == null) {
      return;
    }

    final Object fieldValue =
        iVertexToRemove != null
            ? vertex.getPropertyInternal(fieldName)
            : vertex.removePropertyInternal(fieldName);
    if (fieldValue == null) {
      return;
    }

    if (fieldValue instanceof YTIdentifiable) {
      // SINGLE RECORD

      if (iVertexToRemove != null) {
        if (!fieldValue.equals(iVertexToRemove)) {
          return;
        }
        vertex.setPropertyInternal(fieldName, newVertex);
      }

    } else if (fieldValue instanceof RidBag bag) {
      // COLLECTION OF RECORDS: REMOVE THE ENTRY
      boolean found = false;
      final Iterator<YTIdentifiable> it = bag.iterator();
      while (it.hasNext()) {
        if (it.next().equals(iVertexToRemove)) {
          // REMOVE THE OLD ENTRY
          found = true;
          it.remove();
        }
      }
      if (found)
      // ADD THE NEW ONE
      {
        bag.add(newVertex);
      }

    } else if (fieldValue instanceof Collection) {
      @SuppressWarnings("unchecked") final Collection<YTIdentifiable> col = (Collection<YTIdentifiable>) fieldValue;

      if (col.remove(iVertexToRemove)) {
        col.add(newVertex);
      }
    }

    vertex.save();
  }

  static void deleteLinks(YTVertex delegate) {
    Iterable<YTEdge> allEdges = delegate.getEdges(ODirection.BOTH);
    List<YTEdge> items = new ArrayList<>();
    for (YTEdge edge : allEdges) {
      items.add(edge);
    }
    for (YTEdge edge : items) {
      edge.delete();
    }
  }

  @Override
  default YTRID moveTo(final String className, final String clusterName) {

    final YTEntityImpl baseDoc = getBaseDocument();
    var db = baseDoc.getSession();
    if (!db.getTransaction().isActive()) {
      throw new YTDatabaseException("This operation is allowed only inside a transaction");
    }
    if (checkDeletedInTx(getIdentity())) {
      throw new YTRecordNotFoundException(
          getIdentity(), "The vertex " + getIdentity() + " has been deleted");
    }

    final YTRID oldIdentity = getIdentity().copy();

    final YTRecord oldRecord = oldIdentity.getRecord();
    var doc = baseDoc.copy();
    ORecordInternal.setIdentity(doc, new ChangeableRecordId());

    // DELETE THE OLD RECORD FIRST TO AVOID ISSUES WITH UNIQUE CONSTRAINTS
    copyRidBags(db, oldRecord, doc);
    detachRidbags(oldRecord);
    db.delete(oldRecord);

    var delegate = new YTVertexDelegate(doc);
    final Iterable<YTEdge> outEdges = delegate.getEdges(ODirection.OUT);
    final Iterable<YTEdge> inEdges = delegate.getEdges(ODirection.IN);
    if (className != null) {
      doc.setClassName(className);
    }

    // SAVE THE NEW VERTEX
    doc.setDirty();

    ORecordInternal.setIdentity(doc, new ChangeableRecordId());
    db.save(doc, clusterName);
    if (db.getTransaction().getEntryCount() == 2) {
      System.out.println("WTF");
      db.save(doc, clusterName);
    }
    final YTRID newIdentity = doc.getIdentity();

    // CONVERT OUT EDGES
    for (YTEdge oe : outEdges) {
      final YTIdentifiable inVLink = oe.getVertexLink(ODirection.IN);
      var optSchemaType = oe.getSchemaType();

      String schemaType;
      //noinspection OptionalIsPresent
      if (optSchemaType.isPresent()) {
        schemaType = optSchemaType.get().getName();
      } else {
        schemaType = null;
      }

      final String inFieldName = getEdgeLinkFieldName(ODirection.IN, schemaType, true);

      // link to itself
      YTEntityImpl inRecord;
      if (inVLink.equals(oldIdentity)) {
        inRecord = doc;
      } else {
        inRecord = inVLink.getRecord();
      }
      //noinspection deprecation
      if (oe.isLightweight()) {
        // REPLACE ALL REFS IN inVertex
        replaceLinks(inRecord, inFieldName, oldIdentity, newIdentity);
      } else {
        // REPLACE WITH NEW VERTEX
        ((YTEntityInternal) oe).setPropertyInternal(YTEdgeInternal.DIRECTION_OUT, newIdentity);
      }

      db.save(oe);
    }

    for (YTEdge ine : inEdges) {
      final YTIdentifiable outVLink = ine.getVertexLink(ODirection.OUT);

      var optSchemaType = ine.getSchemaType();

      String schemaType;
      //noinspection OptionalIsPresent
      if (optSchemaType.isPresent()) {
        schemaType = optSchemaType.get().getName();
      } else {
        schemaType = null;
      }

      final String outFieldName = getEdgeLinkFieldName(ODirection.OUT, schemaType, true);

      YTEntityImpl outRecord;
      if (outVLink.equals(oldIdentity)) {
        outRecord = doc;
      } else {
        outRecord = outVLink.getRecord();
      }
      //noinspection deprecation
      if (ine.isLightweight()) {
        // REPLACE ALL REFS IN outVertex
        replaceLinks(outRecord, outFieldName, oldIdentity, newIdentity);
      } else {
        // REPLACE WITH NEW VERTEX
        ((YTEdgeInternal) ine).setPropertyInternal(YTEdge.DIRECTION_IN, newIdentity);
      }

      db.save(ine);
    }

    // FINAL SAVE
    db.save(doc);
    return newIdentity;
  }

  private static void detachRidbags(YTRecord oldRecord) {
    YTEntityImpl oldDoc = (YTEntityImpl) oldRecord;
    for (String field : oldDoc.getPropertyNamesInternal()) {
      if (field.equalsIgnoreCase(YTEdgeInternal.DIRECTION_OUT)
          || field.equalsIgnoreCase(YTEdgeInternal.DIRECTION_IN)
          || field.startsWith(DIRECTION_OUT_PREFIX)
          || field.startsWith(DIRECTION_IN_PREFIX)
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof RidBag) {
          oldDoc.removePropertyInternal(field);
        }
      }
    }
  }

  static boolean checkDeletedInTx(YTRID id) {
    var db = ODatabaseRecordThreadLocal.instance().get();
    if (db == null) {
      return false;
    }

    final ORecordOperation oper = db.getTransaction().getRecordEntry(id);
    if (oper == null) {
      return id.isTemporary();
    } else {
      return oper.type == ORecordOperation.DELETED;
    }
  }

  private static void copyRidBags(YTDatabaseSessionInternal db, YTRecord oldRecord,
      YTEntityImpl newDoc) {
    YTEntityImpl oldDoc = (YTEntityImpl) oldRecord;
    for (String field : oldDoc.getPropertyNamesInternal()) {
      if (field.equalsIgnoreCase(YTEdgeInternal.DIRECTION_OUT)
          || field.equalsIgnoreCase(YTEdgeInternal.DIRECTION_IN)
          || field.startsWith(DIRECTION_OUT_PREFIX)
          || field.startsWith(DIRECTION_IN_PREFIX)
          || field.startsWith("OUT_")
          || field.startsWith("IN_")) {
        Object val = oldDoc.rawField(field);
        if (val instanceof RidBag bag) {
          if (!bag.isEmbedded()) {
            RidBag newBag = new RidBag(db);
            for (YTIdentifiable identifiable : bag) {
              newBag.add(identifiable);
            }
            newDoc.setPropertyInternal(field, newBag);
          }
        }
      }
    }
  }

  private static String getConnectionClass(final ODirection iDirection, final String iFieldName) {
    if (iDirection == ODirection.OUT) {
      if (iFieldName.length() > DIRECTION_OUT_PREFIX.length()) {
        return iFieldName.substring(DIRECTION_OUT_PREFIX.length());
      }
    } else if (iDirection == ODirection.IN) {
      if (iFieldName.length() > DIRECTION_IN_PREFIX.length()) {
        return iFieldName.substring(DIRECTION_IN_PREFIX.length());
      }
    }
    return YTEdgeInternal.CLASS_NAME;
  }

  static String getEdgeLinkFieldName(
      final ODirection direction,
      final String className,
      final boolean useVertexFieldsForEdgeLabels) {
    if (direction == null || direction == ODirection.BOTH) {
      throw new IllegalArgumentException("Direction not valid");
    }

    if (useVertexFieldsForEdgeLabels) {
      // PREFIX "out_" or "in_" TO THE FIELD NAME
      final String prefix =
          direction == ODirection.OUT ? DIRECTION_OUT_PREFIX : DIRECTION_IN_PREFIX;
      if (className == null || className.isEmpty() || className.equals(YTEdgeInternal.CLASS_NAME)) {
        return prefix;
      }

      return prefix + className;
    } else
    // "out" or "in"
    {
      return direction == ODirection.OUT ? YTEdgeInternal.DIRECTION_OUT
          : YTEdgeInternal.DIRECTION_IN;
    }
  }

  /**
   * updates old and new vertices connected to an edge after out/in update on the edge itself
   */
  static void changeVertexEdgePointers(
      YTEntityImpl edge,
      YTIdentifiable prevInVertex,
      YTIdentifiable currentInVertex,
      YTIdentifiable prevOutVertex,
      YTIdentifiable currentOutVertex) {
    var edgeClass = edge.getClassName();

    if (currentInVertex != prevInVertex) {
      changeVertexEdgePointersOneDirection(
          edge, prevInVertex, currentInVertex, edgeClass, ODirection.IN);
    }
    if (currentOutVertex != prevOutVertex) {
      changeVertexEdgePointersOneDirection(
          edge, prevOutVertex, currentOutVertex, edgeClass, ODirection.OUT);
    }
  }

  private static void changeVertexEdgePointersOneDirection(
      YTEntityImpl edge,
      YTIdentifiable prevInVertex,
      YTIdentifiable currentInVertex,
      String edgeClass,
      ODirection direction) {
    if (prevInVertex != null) {
      var inFieldName = YTVertex.getEdgeLinkFieldName(direction, edgeClass);
      var prevRecord = prevInVertex.<YTEntityImpl>getRecord();

      var prevLink = prevRecord.getPropertyInternal(inFieldName);
      if (prevLink != null) {
        removeVertexLink(prevRecord, inFieldName, prevLink, edgeClass, edge);
      }

      var currentRecord = currentInVertex.<YTEntityImpl>getRecord();
      createLink(currentRecord, edge, inFieldName);

      prevRecord.save();
      currentRecord.save();
    }
  }

  private static String[] resolveAliases(YTSchema schema, String[] labels) {
    if (labels == null) {
      return null;
    }
    String[] result = new String[labels.length];

    for (int i = 0; i < labels.length; i++) {
      result[i] = resolveAlias(labels[i], schema);
    }

    return result;
  }

  private static String resolveAlias(String label, YTSchema schema) {
    YTClass clazz = schema.getClass(label);
    if (clazz != null) {
      return clazz.getName();
    }

    return label;
  }


  private static void removeVertexLink(
      YTEntityInternal vertex,
      String fieldName,
      Object link,
      String label,
      YTIdentifiable identifiable) {
    if (link instanceof Collection) {
      ((Collection<?>) link).remove(identifiable);
    } else if (link instanceof RidBag) {
      ((RidBag) link).remove(identifiable);
    } else if (link instanceof YTIdentifiable && link.equals(vertex)) {
      vertex.removePropertyInternal(fieldName);
    } else {
      throw new IllegalArgumentException(
          label + " is not a valid link in vertex with rid " + vertex.getIdentity());
    }
  }

  /**
   * Creates a link between a vertices and a Graph Element.
   */
  static void createLink(
      final YTEntityImpl fromVertex, final YTIdentifiable to, final String fieldName) {
    final Object out;
    YTType outType = fromVertex.fieldType(fieldName);
    Object found = fromVertex.getPropertyInternal(fieldName);

    final YTClass linkClass = ODocumentInternal.getImmutableSchemaClass(fromVertex);
    if (linkClass == null) {
      throw new IllegalArgumentException("Class not found in source vertex: " + fromVertex);
    }

    final YTProperty prop = linkClass.getProperty(fieldName);
    final YTType propType = prop != null && prop.getType() != YTType.ANY ? prop.getType() : null;

    if (found == null) {
      if (propType == YTType.LINKLIST
          || (prop != null
          && "true".equalsIgnoreCase(prop.getCustom("ordered")))) { // TODO constant
        var coll = new LinkList(fromVertex);
        coll.add(to);
        out = coll;
        outType = YTType.LINKLIST;
      } else if (propType == null || propType == YTType.LINKBAG) {
        final RidBag bag = new RidBag(fromVertex.getSession());
        bag.add(to);
        out = bag;
        outType = YTType.LINKBAG;
      } else if (propType == YTType.LINK) {
        out = to;
        outType = YTType.LINK;
      } else {
        throw new YTDatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for link creation.");
      }

    } else if (found instanceof YTIdentifiable foundId) {
      if (prop != null && propType == YTType.LINK) {
        throw new YTDatabaseException(
            "Type of field provided in schema '"
                + prop.getType()
                + "' cannot be used for creation to hold several links.");
      }

      if (prop != null && "true".equalsIgnoreCase(prop.getCustom("ordered"))) { // TODO constant
        var coll = new LinkList(fromVertex);
        coll.add(foundId);
        coll.add(to);
        out = coll;
        outType = YTType.LINKLIST;
      } else {
        final RidBag bag = new RidBag(fromVertex.getSession());
        bag.add(foundId);
        bag.add(to);
        out = bag;
        outType = YTType.LINKBAG;
      }
    } else if (found instanceof RidBag) {
      // ADD THE LINK TO THE COLLECTION
      out = null;

      ((RidBag) found).add(to.getRecord());

    } else if (found instanceof Collection<?>) {
      // USE THE FOUND COLLECTION
      out = null;
      //noinspection unchecked
      ((Collection<YTIdentifiable>) found).add(to);

    } else {
      throw new YTDatabaseException(
          "Relationship content is invalid on field " + fieldName + ". Found: " + found);
    }

    if (out != null)
    // OVERWRITE IT
    {
      fromVertex.setPropertyInternal(fieldName, out, outType);
    }
  }

  private static void removeLinkFromEdge(YTEntityImpl vertex, YTEdge edge, ODirection direction) {
    var schemaType = edge.getSchemaType();
    assert schemaType.isPresent();

    String className = schemaType.get().getName();
    YTIdentifiable edgeId = edge.getIdentity();

    removeLinkFromEdge(
        vertex, edge, YTVertex.getEdgeLinkFieldName(direction, className), edgeId, direction);
  }

  private static void removeLinkFromEdge(
      YTEntityImpl vertex, YTEdge edge, String edgeField, YTIdentifiable edgeId,
      ODirection direction) {
    Object edgeProp = vertex.getPropertyInternal(edgeField);
    YTRID oppositeVertexId = null;
    if (direction == ODirection.IN) {
      var fromIdentifiable = edge.getFromIdentifiable();
      if (fromIdentifiable != null) {
        oppositeVertexId = fromIdentifiable.getIdentity();
      }
    } else {
      var toIdentifiable = edge.getToIdentifiable();
      if (toIdentifiable != null) {
        oppositeVertexId = toIdentifiable.getIdentity();
      }
    }

    if (edgeId == null) {
      // lightweight edge
      edgeId = oppositeVertexId;
    }

    removeEdgeLinkFromProperty(vertex, edge, edgeField, edgeId, edgeProp);
  }

  private static void removeEdgeLinkFromProperty(
      YTEntityImpl vertex, YTEdge edge, String edgeField, YTIdentifiable edgeId, Object edgeProp) {
    if (edgeProp instanceof Collection) {
      ((Collection<?>) edgeProp).remove(edgeId);
    } else if (edgeProp instanceof RidBag) {
      ((RidBag) edgeProp).remove(edgeId);
    } else //noinspection deprecation
      if (edgeProp instanceof YTIdentifiable
          && ((YTIdentifiable) edgeProp).getIdentity() != null
          && ((YTIdentifiable) edgeProp).getIdentity().equals(edgeId)
          || edge.isLightweight()) {
        vertex.removePropertyInternal(edgeField);
      } else {
        OLogManager.instance()
            .warn(
                vertex,
                "Error detaching edge: the vertex collection field is of type "
                    + (edgeProp == null ? "null" : edgeProp.getClass()));
      }
  }

  static void removeIncomingEdge(YTVertex vertex, YTEdge edge) {
    removeLinkFromEdge(((YTVertexInternal) vertex).getBaseDocument(), edge, ODirection.IN);
  }

  static void removeOutgoingEdge(YTVertex vertex, YTEdge edge) {
    removeLinkFromEdge(((YTVertexInternal) vertex).getBaseDocument(), edge, ODirection.OUT);
  }
}
