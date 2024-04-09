/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.QueryVisitor;
import org.apache.lucene.search.ScoreMode;
import org.apache.lucene.search.Weight;

/**
 * Implementation of Query interface for type "hybrid". It allows execution of multiple sub-queries and collect individual
 * scores for each sub-query.
 */
@AllArgsConstructor
@Getter
@NonNull
public final class NeuralSparseQuery extends Query {

    private Query currentQuery;
    private final Query highScoreTokenQuery;
    private final Query lowScoreTokenQuery;
    private final int twoPhaseWindowSize;

    @Override
    public String toString(String field) {
        return "NeuralSparseQuery(" + highScoreTokenQuery.toString(field) + ", " + lowScoreTokenQuery.toString(field) + ")";
    }

    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        Query rewrittenCurrentQuery = currentQuery.rewrite(indexSearcher);
        Query rewrittenFirstStepQuery = highScoreTokenQuery.rewrite(indexSearcher);
        Query rewrittenSecondPhaseQuery = lowScoreTokenQuery.rewrite(indexSearcher);
        if (rewrittenFirstStepQuery == highScoreTokenQuery && rewrittenSecondPhaseQuery == lowScoreTokenQuery) {
            return this;
        }
        return new NeuralSparseQuery(rewrittenCurrentQuery, rewrittenFirstStepQuery, rewrittenSecondPhaseQuery, this.twoPhaseWindowSize);
    }

    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        highScoreTokenQuery.visit(v);
        lowScoreTokenQuery.visit(v);
    }

    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(NeuralSparseQuery other) {
        return Objects.equals(highScoreTokenQuery, other.highScoreTokenQuery)
            && Objects.equals(lowScoreTokenQuery, other.lowScoreTokenQuery);
    }

    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(highScoreTokenQuery) + Objects.hashCode(lowScoreTokenQuery);
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        return currentQuery.createWeight(searcher, scoreMode, boost);
    }

    public void extractLowScoreToken() {
        this.currentQuery = highScoreTokenQuery;
    }
}
