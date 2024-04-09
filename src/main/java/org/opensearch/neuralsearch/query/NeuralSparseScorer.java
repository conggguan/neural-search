/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.neuralsearch.query;

import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.ScorerSupplier;
import org.apache.lucene.search.TwoPhaseIterator;

import java.io.IOException;
import java.util.PriorityQueue;

@Log4j2
public class NeuralSparseScorer extends Scorer {
    private final Scorer highScorer;
    boolean sup = false;
    private ScorerSupplier lowScorerSupplier;
    private final PriorityQueue<Float> topScoreQueue;
    private final DocIdSetIterator iterator;

    protected NeuralSparseScorer(ScorerSupplier highScorerSupplier, NeuralSparseQueryWeight weight, PriorityQueue<Float> queue)
        throws IOException {
        super(weight);
        this.highScorer = highScorerSupplier.get(Long.MAX_VALUE);
        this.iterator = highScorer.iterator();
        this.topScoreQueue = queue;
    }

    @Override
    public int docID() {
        return highScorer.docID();
    }

    @Override
    public float score() throws IOException {
        if (highScorer != null) {
            float highTokenScore = highScorer.score();
            float finalScore = highTokenScore;
            if (updateScore(highTokenScore) && lowScorerSupplier != null) {
                if (!sup) lowScorerSupplier = ((NeuralSparseQueryWeight) weight).getLowTokenSS();
                finalScore += lowScorerSupplier.get(Long.MAX_VALUE).score();
            }
            return finalScore;
        }
        return 0.0f;
    }

    @Override
    public DocIdSetIterator iterator() {
        return this.iterator;
    }

    @Override
    public float getMaxScore(int upTo) throws IOException {
        return highScorer.getMaxScore(upTo);
    }

    public boolean updateScore(Float element) {
        if (topScoreQueue.size() < 50) {
            topScoreQueue.add(element);
            return true;
        } else {
            if (element.compareTo(topScoreQueue.peek()) > 0) {
                topScoreQueue.poll(); // 移除最小元素
                topScoreQueue.add(element);
                return true;
            }
            return false;
        }
    }

    @Override
    public TwoPhaseIterator twoPhaseIterator() {
        return highScorer.twoPhaseIterator();
    }

    public int advanceShallow(int target) throws IOException {
        return highScorer.advanceShallow(target);
    }

}
