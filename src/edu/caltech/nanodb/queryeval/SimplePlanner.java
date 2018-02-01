package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.plannodes.*;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.queryast.SelectValue;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.expressions.AggregateProcessor;

import edu.caltech.nanodb.relations.TableInfo;


/**
 * This class generates execution plans for very simple SQL
 * <tt>SELECT * FROM tbl [WHERE P]</tt> queries.  The primary responsibility
 * is to generate plans for SQL <tt>SELECT</tt> statements, but
 * <tt>UPDATE</tt> and <tt>DELETE</tt> expressions will also use this class
 * to generate simple plans to identify the tuples to update or delete.
 */
public class SimplePlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(SimplePlanner.class);


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    @Override
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // For HW1, we have a very simple implementation that defers to
        // makeSimpleSelect() to handle simple SELECT queries with one table,
        // and an optional WHERE clause.

        // Process the aggregate commands.
        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }
        // Traverse and process the select values to search for any aggregate
        // functions.
        List<SelectValue> selectValues = selClause.getSelectValues();
        AggregateProcessor processor = new AggregateProcessor();
        boolean hasAgg = false;

        for (SelectValue sv : selectValues) {
            // Skip select-values that aren't expressions
            if (!sv.isExpression()){
                continue;
            }
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
            // Raise hasAggregate flag if we've encountered an aggregate function.
            if (e.toString().contains("aggrfn")){
                hasAgg = true;
            }
        }

        // get the from clause
        // logger.warn("getting from");
        FromClause fromClause = selClause.getFromClause();

        // if no from clause
        if(fromClause == null)
        {
            // can use a project here
            ProjectNode projectNode = new ProjectNode(selClause.getSelectValues());
            // logger.warn(String.format("project %s", projectNode.toString()));

            projectNode.prepare();

            return projectNode;

        }


        // logger.warn(String.format("got from, %s", fromClause.toString()));

        PlanNode plan;
        // if from exists
        // either get the tables and join on a where (filter)

        // if from clause has select clause
        if (fromClause.isDerivedTable()) {
            // get child recursively
            // logger.warn("derived");
            PlanNode childNode = makePlan(fromClause.getSelectClause(),
                    enclosingSelects);
            // logger.warn(String.format("plan = %s", childNode.toString()));

            // update the plan
            plan = new RenameNode(childNode, fromClause.getResultName());
            plan.prepare();
            // logger.warn(String.format("plan = %s", plan.toString()));
        }

        // join expression
        else if(fromClause.isJoinExpr()) {
            // logger.warn("join");

            // if FROM clause has join and aggregate in "ON" expression,
            // throw an IllegalArgumentException.
            if (fromClause.getOnExpression() != null){
                Expression onEx = fromClause.getOnExpression().traverse(processor);
                if (onEx.toString().contains("aggrfn")){
                    throw new IllegalArgumentException("On clause can't have aggregate functions");
                }
            }

            // compute the plan by joining
            plan = computeJoinClauses(fromClause, enclosingSelects);
        }


        // base table
        else {
            // can just use makesimpleselect
            // logger.warn("is base table");
            plan = makeSimpleSelect(fromClause.getTableName(),
                    selClause.getWhereExpr(), null);
        }

        // check for where clause
        if (selClause.getWhereExpr() != null) {
            // if WHERE clause has aggregate function in the expression,
            // throw an IllegalArgumentException.
            Expression onEx = selClause.getWhereExpr().traverse(processor);
            if (onEx.toString().contains("aggrfn")) {
                throw new IllegalArgumentException("WHERE clause can't have aggregate functions");
            }

            // otherwise generate the plan
            // logger.warn("get where table");
            PlanNode oldPlan = plan;
            plan = new SimpleFilterNode(oldPlan, selClause.getWhereExpr());
            plan.prepare();
            // logger.warn("end where table");

        }

        // Create Grouping and Aggregate Node, if necessary
        if(hasAgg || selClause.getGroupByExprs().size() > 0) {
            PlanNode aggNode = plan;
            plan = new HashedGroupAggregateNode(aggNode,
                    selClause.getGroupByExprs(), processor.getAggregates());
            plan.prepare();
        }

        // if project is not trivial, add a node to the hierarchy
        if (!selClause.isTrivialProject()) {
            // throw new UnsupportedOperationException("Not implemented:  project");
            // logger.warn("nontrivial project");
            PlanNode oldPlan = plan;
            plan = new ProjectNode(oldPlan, selectValues);
            plan.prepare();

        }

        // this should go last, as we order last
        if(selClause.getOrderByExprs().size() > 0)
        {
            // logger.warn("order by");
            PlanNode sNode = plan;
            plan = new SortNode(sNode, selClause.getOrderByExprs());
            plan.prepare();
        }

        // now return the plan
        // logger.warn(String.format("return %s", plan.toString()));
        return plan;
    }


    // helper function to compute our join clauses
    public PlanNode computeJoinClauses(FromClause fromClause,
                                       List<SelectClause> enclosingSelects)
            throws IOException {

        // get renames working here
        PlanNode right;
        PlanNode left;
        PlanNode temp;

        // test cases and make recursive calls when necessary
        if(fromClause.getLeftChild().isJoinExpr()) {
            temp = computeJoinClauses(fromClause.getLeftChild(), enclosingSelects);
            // temp = makePlan(fromClause.getLeftChild().getSelectClause(), enclosingSelects);

        }
        else if(fromClause.getLeftChild().isDerivedTable()) {
            temp = makePlan(fromClause.getLeftChild().getSelectClause(), enclosingSelects);
        }

        // base table
        else {
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getLeftChild().getTableName());
            temp = new FileScanNode(tableInfo, null);
            // left = new RenameNode(temp, fromClause.getLeftChild().getResultName());
        }

        left = new RenameNode(temp, fromClause.getLeftChild().getResultName());

        if(fromClause.getRightChild().isJoinExpr()) {
            temp = computeJoinClauses(fromClause.getRightChild(), enclosingSelects);
            // temp = makePlan(fromClause.getRightChild().getSelectClause(), enclosingSelects);
        }

        else if(fromClause.getRightChild().isDerivedTable()) {
            temp = makePlan(fromClause.getRightChild().getSelectClause(), enclosingSelects);
        }

        // base table
        else {
            // set right to something
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getRightChild().getTableName());
            // PlanNode temp;
            temp = new FileScanNode(tableInfo, null);
            // temp = new RenameNode(temp, fromClause.getRightChild().getResultName());
        }

        right = new RenameNode(temp, fromClause.getRightChild().getResultName());

        // check the join conditions and join
        NestedLoopJoinNode joinNode = new NestedLoopJoinNode(left, right,
                fromClause.getJoinType(), fromClause.getOnExpression());
        joinNode.prepare();

        return joinNode;

    }


    /**
     * Constructs a simple select plan that reads directly from a table, with
     * an optional predicate for selecting rows.
     * <p>
     * While this method can be used for building up larger <tt>SELECT</tt>
     * queries, the returned plan is also suitable for use in <tt>UPDATE</tt>
     * and <tt>DELETE</tt> command evaluation.  In these cases, the plan must
     * only generate tuples of type {@link edu.caltech.nanodb.storage.PageTuple},
     * so that the command can modify or delete the actual tuple in the file's
     * page data.
     *
     * @param tableName The name of the table that is being selected from.
     *
     * @param predicate An optional selection predicate, or {@code null} if
     *        no filtering is desired.
     *
     * @return A new plan-node for evaluating the select operation.
     *
     * @throws IOException if an error occurs when loading necessary table
     *         information.
     */
    public SelectNode makeSimpleSelect(String tableName, Expression predicate,
        List<SelectClause> enclosingSelects) throws IOException {
        if (tableName == null)
            throw new IllegalArgumentException("tableName cannot be null");

        if (enclosingSelects != null) {
            // If there are enclosing selects, this subquery's predicate may
            // reference an outer query's value, but we don't detect that here.
            // Therefore we will probably fail with an unrecognized column
            // reference.
            logger.warn("Currently we are not clever enough to detect " +
                "correlated subqueries, so expect things are about to break...");
        }

        // Open the table.
        TableInfo tableInfo = storageManager.getTableManager().openTable(tableName);

        // Make a SelectNode to read rows from the table, with the specified
        // predicate.
        SelectNode selectNode = new FileScanNode(tableInfo, predicate);
        selectNode.prepare();
        return selectNode;
    }
}

