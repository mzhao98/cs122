package edu.caltech.nanodb.plannodes;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.queryeval.TableStats;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.OrderByExpression;
import edu.caltech.nanodb.queryeval.ColumnStats;
import edu.caltech.nanodb.queryeval.PlanCost;
import edu.caltech.nanodb.queryeval.SelectivityEstimator;
import edu.caltech.nanodb.relations.JoinType;
import edu.caltech.nanodb.relations.Tuple;
import edu.caltech.nanodb.expressions.TupleLiteral;

import java.util.ArrayList;


/**
 * This plan node implements a nested-loop join operation, which can support
 * arbitrary join conditions but is also the slowest join implementation.
 */
public class NestedLoopJoinNode extends ThetaJoinNode {
    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(NestedLoopJoinNode.class);


    /** Most recently retrieved tuple of the left relation. */
    private Tuple leftTuple;

    /** Most recently retrieved tuple of the right relation. */
    private Tuple rightTuple;


    /** Set to true when we have exhausted all tuples from our subplans. */
    private boolean done;

    /** Set to true when we have a match for left or right joins. */
    private boolean matched;

    /** Set to true when we have null joined a tuple in a left join. */
    private boolean nullJoined;

    /** Tuple literal of all nulls to join for left joins. */
    private TupleLiteral allNulls;


    public NestedLoopJoinNode(PlanNode leftChild, PlanNode rightChild,
                JoinType joinType, Expression predicate) {

        super(leftChild, rightChild, joinType, predicate);

        // We can treat a right outer join like a left outer join with a swap
        // when it is time to project.
        if (joinType == JoinType.RIGHT_OUTER && !isSwapped()){
            swap();
        }

        this.matched = false;

        this.nullJoined = false;
    }


    /**
     * Checks if the argument is a plan node tree with the same structure, but not
     * necessarily the same references.
     *
     * @param obj the object to which we are comparing
     */
    @Override
    public boolean equals(Object obj) {

        if (obj instanceof NestedLoopJoinNode) {
            NestedLoopJoinNode other = (NestedLoopJoinNode) obj;

            return predicate.equals(other.predicate) &&
                leftChild.equals(other.leftChild) &&
                rightChild.equals(other.rightChild);
        }

        return false;
    }


    /** Computes the hash-code of the nested-loop plan node. */
    @Override
    public int hashCode() {
        int hash = 7;
        hash = 31 * hash + (predicate != null ? predicate.hashCode() : 0);
        hash = 31 * hash + leftChild.hashCode();
        hash = 31 * hash + rightChild.hashCode();
        return hash;
    }


    /**
     * Returns a string representing this nested-loop join's vital information.
     *
     * @return a string representing this plan-node.
     */
    @Override
    public String toString() {
        StringBuilder buf = new StringBuilder();

        buf.append("NestedLoop[");

        if (predicate != null)
            buf.append("pred:  ").append(predicate);
        else
            buf.append("no pred");

        if (schemaSwapped)
            buf.append(" (schema swapped)");

        buf.append(']');

        return buf.toString();
    }


    /**
     * Creates a copy of this plan node and its subtrees.
     */
    @Override
    protected PlanNode clone() throws CloneNotSupportedException {
        NestedLoopJoinNode node = (NestedLoopJoinNode) super.clone();

        // Clone the predicate.
        if (predicate != null)
            node.predicate = predicate.duplicate();
        else
            node.predicate = null;

        return node;
    }


    /**
     * Nested-loop joins can conceivably produce sorted results in situations
     * where the outer relation is ordered, but we will keep it simple and just
     * report that the results are not ordered.
     */
    @Override
    public List<OrderByExpression> resultsOrderedBy() {
        return null;
    }


    /** True if the node supports position marking. **/
    public boolean supportsMarking() {
        return leftChild.supportsMarking() && rightChild.supportsMarking();
    }


    /** True if the node requires that its left child supports marking. */
    public boolean requiresLeftMarking() {
        return false;
    }


    /** True if the node requires that its right child supports marking. */
    public boolean requiresRightMarking() {
        return false;
    }


    @Override
    public void prepare() {
        // Need to prepare the left and right child-nodes before we can do
        // our own work.
        leftChild.prepare();
        rightChild.prepare();

        // Once the children are prepared we can create a TupleLiteral
        // for joining a left tuple with a properly null-padded tuple
        // in the case of an outer join which finds no tuples to match
        allNulls = new TupleLiteral(rightChild.getSchema().numColumns());

        // Use the parent class' helper-function to prepare the schema.
        prepareSchemaStats();

        PlanCost rightCost = rightChild.cost;
        PlanCost leftCost = leftChild.cost;

        float selectivity = 1;
        float scale = 1;

        if(predicate != null) {
            selectivity = SelectivityEstimator.estimateSelectivity(predicate,
                    schema, stats);
            scale = 2;
        }
        float cpuCost = scale * leftCost.numTuples * rightCost.numTuples;
        float numTups = selectivity * (leftCost.numTuples * rightCost.numTuples);
        long blockIOs = rightCost.numBlockIOs + leftCost.numBlockIOs;
        float tupleLen = rightCost.tupleSize + leftCost.tupleSize;

        if(joinType == JoinType.LEFT_OUTER || joinType == JoinType.RIGHT_OUTER)
        {
            numTups += leftCost.numTuples;
        }

        cost = new PlanCost(numTups, tupleLen, cpuCost, blockIOs);
    }


    public void initialize() {
        super.initialize();

        done = false;
        leftTuple = null;
        rightTuple = null;
    }


    /**
     * Returns the next joined tuple that satisfies the join condition.
     *
     * @return the next joined tuple that satisfies the join condition.
     *
     * @throws IOException if a db file failed to open at some point
     */
    public Tuple getNextTuple() throws IOException {
        if (done)
            return null;

        while (getTuplesToJoin()) {
            // If we can join the tuples or if the right tuple is the null
            // padded tuple from an outer join we proceed
            if (canJoinTuples() || rightTuple == allNulls) {
                if (leftTuple == null && rightTuple == null) {
                    done = true;
                    return null;
                }

                // In the case of an outer join if we have found a match
                // then set matched to true so we do not null-join the
                // current left tuple
                if (rightTuple != allNulls) {
                    matched = true;
                }

                return joinTuples(leftTuple, rightTuple);
            }
        }
        return null;
    }


    /**
     * This helper function implements the logic that sets {@link #leftTuple}
     * and {@link #rightTuple} based on the nested-loop logic. There are two
     * cases, one with simpler logic for an inner join and another for an
     * outer join.
     *
     * @return {@code true} if another pair of tuples was found to join, or
     *         {@code false} if no more pairs of tuples are available to join.
     */
    private boolean getTuplesToJoin() throws IOException {

        if (joinType == JoinType.INNER || joinType == JoinType.CROSS){
            // If both tuples are null then we initialize and if both tables
            // are not empty then we return true.
            if (leftTuple == null && rightTuple == null){
                logger.warn("Initialize");
                leftTuple = leftChild.getNextTuple();
                rightTuple = rightChild.getNextTuple();
                if (leftTuple != null && rightTuple != null) {
                    return true;
                }
            }

            // If both tuples are not null then we advance the right tuple
            // and if the right tuple is null then we have reached the end
            // of the right table so we advance the left tuple and reset
            // the right tuple. If the left tuple is now null then we are
            // done
            else if (leftTuple != null && rightTuple != null){
                rightTuple = rightChild.getNextTuple();
                if (rightTuple == null) {
                    leftTuple = leftChild.getNextTuple();
                    rightChild.initialize();
                    rightTuple = rightChild.getNextTuple();

                    if (leftTuple == null) {
                        done = true;
                        return false;
                    }
                    return true;
                }
                return true;
            }

            done = true;
            return false;
        }

        // Case for outer joins
        else if (joinType == JoinType.RIGHT_OUTER ||
                joinType == JoinType.LEFT_OUTER) {

            // If we have null joined the left tuple then advance the left
            // tuple and reset the right tuple and the matched and nulljoined
            // tuples. If the left tuple is null then we are done, if the
            // right tuple is null then we nulljoin it with the current
            // left tuple.
            if(nullJoined) {
                leftTuple = leftChild.getNextTuple();
                rightChild.initialize();
                rightTuple = rightChild.getNextTuple();

                matched = false;
                if (leftTuple == null) {
                    done = true;
                    return false;
                }

                nullJoined = false;

                if (rightTuple == null) {
                    rightTuple = allNulls;
                    nullJoined = true;
                }

                return true;
            }

            // If both tuples are null then initialize both tuples, if
            // the left tuple is null we are done, if the right tuple
            // is null then nulljoin it with the left tuple
            if (leftTuple == null && rightTuple == null){
                leftTuple = leftChild.getNextTuple();
                rightTuple = rightChild.getNextTuple();
                if (leftTuple == null) {
                    done = true;
                    return false;
                }
                if (rightTuple == null) {
                    nullJoined = true;
                    rightTuple = allNulls;
                    return true;
                }
            }

            // If the both tuples are not null then advance the right tuple
            // if the right tuple is null and we have not found a match
            // for the current left tuple then nulljoin the two. If we
            // have found a match but the right tuple is null then we
            // advance the left tuple, reset the right tuple,
            // matched and nulljoined, and perform the same checks as
            // when we initialized the tuples.
            if (leftTuple != null && rightTuple != null){
                rightTuple = rightChild.getNextTuple();
                if (rightTuple == null && !nullJoined && !matched) {
                    nullJoined = true;
                    rightTuple = allNulls;
                    return true;
                }

                if (rightTuple == null) {
                    leftTuple = leftChild.getNextTuple();
                    rightChild.initialize();
                    rightTuple = rightChild.getNextTuple();

                    matched = false;
                    if (leftTuple == null) {
                        done = true;
                        return false;
                    }

                    nullJoined = false;

                    if (rightTuple == null) {
                        rightTuple = allNulls;
                        nullJoined = true;
                    }
                    return true;
                }
                return true;
            }
            done = true;
            return false;
        }
        return false;
    }


    private boolean canJoinTuples() {
        // If the predicate was not set, we can always join them!
        if (predicate == null)
            return true;

        environment.clear();
        environment.addTuple(leftSchema, leftTuple);
        environment.addTuple(rightSchema, rightTuple);

        return predicate.evaluatePredicate(environment);
    }


    public void markCurrentPosition() {
        leftChild.markCurrentPosition();
        rightChild.markCurrentPosition();
    }


    public void resetToLastMark() throws IllegalStateException {
        leftChild.resetToLastMark();
        rightChild.resetToLastMark();

        // TODO:  Prepare to reevaluate the join operation for the tuples.
        //        (Just haven't gotten around to implementing this.)
    }


    public void cleanUp() {
        leftChild.cleanUp();
        rightChild.cleanUp();
    }
}
