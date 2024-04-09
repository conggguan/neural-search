/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import java.io.IOException;
import java.util.Objects;

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
@Getter
@NonNull
public final class NeuralSparseQuery extends Query {

    private final Query highTokenQuery;
    private final Query lowTokenQuery;

    public NeuralSparseQuery(Query highTokenQuery, Query lowTokenQuery) {
        this.highTokenQuery = highTokenQuery;
        this.lowTokenQuery = lowTokenQuery;
    }

    @Override
    public String toString(String field) {
        return "NeuralSparseQuery(" + highTokenQuery.toString(field) + ", " + lowTokenQuery.toString(field) + ")";
    }

    /**
     * Re-writes queries into primitive queries. Callers are expected to call rewrite multiple times if necessary,
     * until the rewritten query is the same as the original query.
     */
    public Query rewrite(IndexSearcher indexSearcher) throws IOException {
        // 首先重写两步查询
        Query rewrittenFirstStepQuery = highTokenQuery.rewrite(indexSearcher);
        Query rewrittenSecondPhaseQuery = lowTokenQuery.rewrite(indexSearcher);
        if (rewrittenFirstStepQuery == highTokenQuery && rewrittenSecondPhaseQuery == lowTokenQuery) {
            return this;
        }
        return new NeuralSparseQuery(rewrittenFirstStepQuery, rewrittenSecondPhaseQuery);
    }

    /**
     * Recurse through the query tree, visiting all child queries and execute provided visitor. Part of multiple
     * standard workflows, e.g. IndexSearcher.rewrite
     *
     * @param queryVisitor a QueryVisitor to be called by each query in the tree
     */
    @Override
    public void visit(QueryVisitor queryVisitor) {
        QueryVisitor v = queryVisitor.getSubVisitor(BooleanClause.Occur.SHOULD, this);
        highTokenQuery.visit(v);
        lowTokenQuery.visit(v);
    }

    /**
     * Override and implement query instance equivalence properly in a subclass. This is required so that QueryCache works properly.
     *
     * @param other query object that when compare with this query object
     * @return
     */
    @Override
    public boolean equals(Object other) {
        return sameClassAs(other) && equalsTo(getClass().cast(other));
    }

    private boolean equalsTo(NeuralSparseQuery other) {
        return Objects.equals(highTokenQuery, other.highTokenQuery) && Objects.equals(lowTokenQuery, other.lowTokenQuery);
    }

    /**
     * Override and implement query hash code properly in a subclass. This is required so that QueryCache works properly.
     *
     * @return hash code of this object
     */
    @Override
    public int hashCode() {
        int h = classHash();
        h = 31 * h + Objects.hashCode(highTokenQuery) + Objects.hashCode(lowTokenQuery);
        return h;
    }

    @Override
    public Weight createWeight(IndexSearcher searcher, ScoreMode scoreMode, float boost) throws IOException {
        highScoreTokenQueryWeight = highTokenQuery.createWeight(searcher, scoreMode, boost);
        return new NeuralSparseQueryWeight(lowTokenQuery, searcher, scoreMode, boost, highScoreTokenQueryWeight, this);
    }

    private Weight highScoreTokenQueryWeight;

}
