package edu.caltech.nanodb.queryeval;


import java.io.IOException;
import java.util.List;

import edu.caltech.nanodb.plannodes.*;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;

import edu.caltech.nanodb.expressions.Expression;

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

        if (enclosingSelects != null && !enclosingSelects.isEmpty()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  enclosing queries");
        }

        // from clause
        // (joins)
        // where (filter)
        //  group by hashed group aggregate
        //  having
        //  order by
        // select (project)

        FromClause fromClause = selClause.getFromClause();
        PlanNode plan;
        // if from exists
        // either get the tables and join on a where (filter)
        if (fromClause.isBaseTable()) {
            // look through all the tuples

            // just call makeSimpleSelect
            plan = makeSimpleSelect(fromClause.getTableName(),
                    selClause.getWhereExpr(), null);
            logger.warn("is base table");

        }

        else if (fromClause.isDerivedTable()) {
            // call recursively
            logger.warn("derived");
            PlanNode childNode = makePlan(fromClause.getSelectClause(),
                    enclosingSelects);
            plan = new SimpleFilterNode(childNode, selClause.getWhereExpr());
            
            plan.initialize();
        }

        // join expression
        else {
            plan = computeJoinClauses(fromClause, enclosingSelects);
        }


//        if (selClause.getWhereExpr() != null) {
//            logger.warn("get where table");
//            PlanNode oldPlan = plan;
//            plan = new SimpleFilterNode(oldPlan, selClause.getWhereExpr());
//            logger.warn("end where table");
//
//        }

        if (!selClause.isTrivialProject()) {
            throw new UnsupportedOperationException(
                    "Not implemented:  project");
            // ProjectNode projectNode = new ProjectNode(selClause.getSelectValues());
            // curPlan = projectNode; // the project will store all the children

        }


//        if (!fromClause.isBaseTable()) {
//            throw new UnsupportedOperationException(
//                    "Not implemented:  joins or subqueries in FROM clause");
//
//
//        }
        logger.warn(String.format("return %s", plan.toString()));
        return plan;
        // return makeSimpleSelect(fromClause.getTableName(), selClause.getWhereExpr(), null);
    }


    public PlanNode computeJoinClauses(FromClause fromClause, List<SelectClause> enclosingSelects)
            throws IOException {

        PlanNode right;
        PlanNode left;

        if(fromClause.getLeftChild().isJoinExpr()) {
            left = computeJoinClauses(fromClause.getLeftChild(), enclosingSelects);
        }
        else if(fromClause.getLeftChild().isDerivedTable()) {
            left = makePlan(fromClause.getRightChild().getSelectClause(), enclosingSelects);
        }

        // base table
        else {
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getLeftChild().getTableName());
            left = new FileScanNode(tableInfo, null);
        }

        if(fromClause.getRightChild().isJoinExpr()) {
            right = computeJoinClauses(fromClause.getRightChild(), enclosingSelects);
        }

        else if(fromClause.getRightChild().isDerivedTable()) {
            right = makePlan(fromClause.getRightChild().getSelectClause(), enclosingSelects);
        }

        // base table
        else {
            // set right to something
            TableInfo tableInfo = storageManager.getTableManager().openTable(fromClause.getRightChild().getTableName());
            right = new FileScanNode(tableInfo, null);
        }


        // check the join conditions
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

