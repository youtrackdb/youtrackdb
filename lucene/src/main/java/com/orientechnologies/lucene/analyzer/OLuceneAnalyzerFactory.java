package com.orientechnologies.lucene.analyzer;

import com.jetbrains.youtrack.db.internal.common.exception.YTException;
import com.jetbrains.youtrack.db.internal.common.log.LogManager;
import com.jetbrains.youtrack.db.internal.core.index.OIndexDefinition;
import com.jetbrains.youtrack.db.internal.core.index.YTIndexException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.Locale;
import java.util.Map;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.standard.StandardAnalyzer;

/**
 *
 */
public class OLuceneAnalyzerFactory {

  public Analyzer createAnalyzer(
      final OIndexDefinition index, final AnalyzerKind kind, final Map<String, ?> metadata) {
    if (index == null) {
      throw new IllegalArgumentException("Index must not be null");
    }
    if (kind == null) {
      throw new IllegalArgumentException("Analyzer kind must not be null");
    }
    if (metadata == null) {
      throw new IllegalArgumentException("Metadata must not be null");
    }
    final String defaultAnalyzerFQN = (String) metadata.get("default");
    final String prefix = index.getClassName() + ".";

    final OLucenePerFieldAnalyzerWrapper analyzer =
        geLucenePerFieldPresetAnalyzerWrapperForAllFields(defaultAnalyzerFQN);
    setDefaultAnalyzerForRequestedKind(index, kind, metadata, prefix, analyzer);
    setSpecializedAnalyzersForEachField(index, kind, metadata, prefix, analyzer);
    return analyzer;
  }

  private OLucenePerFieldAnalyzerWrapper geLucenePerFieldPresetAnalyzerWrapperForAllFields(
      final String defaultAnalyzerFQN) {
    if (defaultAnalyzerFQN == null) {
      return new OLucenePerFieldAnalyzerWrapper(new StandardAnalyzer());
    } else {
      return new OLucenePerFieldAnalyzerWrapper(buildAnalyzer(defaultAnalyzerFQN));
    }
  }

  private void setDefaultAnalyzerForRequestedKind(
      final OIndexDefinition index,
      final AnalyzerKind kind,
      final Map<String, ?> metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) {
    final String specializedAnalyzerFQN = (String) metadata.get(kind.toString());
    if (specializedAnalyzerFQN != null) {
      for (final String field : index.getFields()) {
        analyzer.add(field, buildAnalyzer(specializedAnalyzerFQN));
        analyzer.add(prefix + field, buildAnalyzer(specializedAnalyzerFQN));
      }
    }
  }

  private void setSpecializedAnalyzersForEachField(
      final OIndexDefinition index,
      final AnalyzerKind kind,
      final Map<String, ?> metadata,
      final String prefix,
      final OLucenePerFieldAnalyzerWrapper analyzer) {
    for (final String field : index.getFields()) {
      final String analyzerName = field + "_" + kind.toString();
      final String analyzerStopwords = analyzerName + "_stopwords";

      if (metadata.containsKey(analyzerName) && metadata.containsKey(analyzerStopwords)) {
        @SuppressWarnings("unchecked") final Collection<String> stopWords =
            (Collection<String>) metadata.get(analyzerStopwords);
        analyzer.add(field, buildAnalyzer((String) metadata.get(analyzerName), stopWords));
        analyzer.add(prefix + field, buildAnalyzer((String) metadata.get(analyzerName), stopWords));
      } else if (metadata.containsKey(analyzerName)) {
        analyzer.add(field, buildAnalyzer((String) metadata.get(analyzerName)));
        analyzer.add(prefix + field, buildAnalyzer((String) metadata.get(analyzerName)));
      }
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN) {
    try {
      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getConstructor();
      return (Analyzer) constructor.newInstance();
    } catch (final ClassNotFoundException e) {
      throw YTException.wrapException(
          new YTIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (final NoSuchMethodException e) {
      Class classAnalyzer = null;
      try {
        classAnalyzer = Class.forName(analyzerFQN);
        return (Analyzer) classAnalyzer.newInstance();
      } catch (Exception e1) {
        LogManager.instance().error(this, "Exception is suppressed, original exception is ", e);
        //noinspection ThrowInsideCatchBlockWhichIgnoresCaughtException
        throw YTException.wrapException(
            new YTIndexException("Couldn't instantiate analyzer:  public constructor  not found"),
            e1);
      }
    } catch (Exception e) {
      LogManager.instance()
          .error(
              this,
              "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)",
              e);
      return new StandardAnalyzer();
    }
  }

  private Analyzer buildAnalyzer(final String analyzerFQN, final Collection<String> stopwords) {
    try {
      final Class classAnalyzer = Class.forName(analyzerFQN);
      final Constructor constructor = classAnalyzer.getDeclaredConstructor(CharArraySet.class);
      return (Analyzer) constructor.newInstance(new CharArraySet(stopwords, true));
    } catch (final ClassNotFoundException e) {
      throw YTException.wrapException(
          new YTIndexException("Analyzer: " + analyzerFQN + " not found"), e);
    } catch (final NoSuchMethodException e) {
      throw YTException.wrapException(
          new YTIndexException("Couldn't instantiate analyzer: public constructor not found"), e);
    } catch (final Exception e) {
      LogManager.instance()
          .error(
              this,
              "Error on getting analyzer for Lucene index (continuing with StandardAnalyzer)",
              e);
      return new StandardAnalyzer();
    }
  }

  public enum AnalyzerKind {
    INDEX,
    QUERY;

    @Override
    public String toString() {
      return name().toLowerCase(Locale.ENGLISH);
    }
  }
}
