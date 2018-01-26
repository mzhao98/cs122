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

        List<SelectValue> selectValues = selClause.getSelectValues();
        AggregateProcessor processor = new AggregateProcessor();
        boolean hasAgg = false;

        //for (int s = 0; s < selectValues.size(); s++){
        for (SelectValue sv : selectValues) {

                // Skip select-values that aren't expressions
            //SelectValue sv= selectValues.get(s);
            if (!sv.isExpression()){
                continue;
            }
            Expression e = sv.getExpression().traverse(processor);
            sv.setExpression(e);
            if (e.toString().contains("aggrfn")){
                hasAgg = true;
            }
        }




        // from clause
        // (joins)
        // where (filter)
        //  group by hashed group aggregate
        //  having
        //  order by
        // select (project)


        // Ask at oh - how to create a new node with the child as a plan node

        // looping through the projects
        // passing the where through the join

        logger.warn("getting from");
        FromClause fromClause = selClause.getFromClause();

        if(fromClause == null)
        {
            // this is the case where we need to rename the table, since no table
            logger.warn(String.format("no from %s", selClause.toString()));
//            String tableName = fromClause.getTableName();
//            selClause.getSelectValues();


            // do we create a table
            // TableInfo tableInfo = storageManager.getTableManager().createTable("name");

            // using a schema
            ProjectNode projectNode = new ProjectNode(selClause.getSelectValues());
            logger.warn(String.format("clasue %s", selClause.getSelectValues().toString()));
            logger.warn(String.format("project %s", projectNode.toString()));

            projectNode.prepare();

            return projectNode;

            // do we have to parse the values, if so, how do we do it
            // do we loop through the string


            // if(selClause.)
        }


        logger.warn(String.format("got from, %s", fromClause.toString()));

        PlanNode plan;
        // if from exists
        // either get the tables and join on a where (filter)


        if (fromClause.isDerivedTable()) {
            // call recursively
            logger.warn("derived");
            PlanNode childNode = makePlan(fromClause.getSelectClause(),
                    enclosingSelects);
            logger.warn(String.format("plan = %s", childNode.toString()));

            plan = new RenameNode(childNode, fromClause.getResultName());
            plan.prepare();
            logger.warn(String.format("plan = %s", plan.toString()));
        }

        // join expression
        else if(fromClause.isJoinExpr()) {
            plan = computeJoinClauses(fromClause, enclosingSelects);
        }


        // base table
        else {
            // look through all the tuples
            logger.warn("is base table");

            // just call makeSimpleSelect
            plan = makeSimpleSelect(fromClause.getTableName(),
                    selClause.getWhereExpr(), null);
        }

        // check for where
        if (selClause.getWhereExpr() != null) {
            logger.warn("get where table");
            PlanNode oldPlan = plan;
            plan = new SimpleFilterNode(oldPlan, selClause.getWhereExpr());
            plan.prepare();
            logger.warn("end where table");

        }

        if(hasAgg || selClause.getGroupByExprs().size() > 0){
            PlanNode aggNode = plan;
            plan = new HashedGroupAggregateNode(aggNode,
                    selClause.getGroupByExprs(), processor.getAggregates());
            plan.prepare();
        }

        if (!selClause.isTrivialProject()) {
            // throw new UnsupportedOperationException("Not implemented:  project");
            logger.warn("nontrivial project");
            PlanNode oldPlan = plan;
            plan = new ProjectNode(oldPlan, selectValues);
            plan.prepare();

        }


        // this should go last
        if(selClause.getOrderByExprs().size() > 0)
        {
            logger.warn("order by");
            PlanNode sNode = plan;
            plan = new SortNode(sNode, selClause.getOrderByExprs());
            plan.prepare();
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

        // get renames working here
        PlanNode right;
        PlanNode left;
        PlanNode temp;

        // probably need to pass in where clause

        if(fromClause.getLeftChild().isJoinExpr()) {
            // need to rename
            temp = computeJoinClauses(fromClause.getLeftChild(), enclosingSelects);
        }
        else if(fromClause.getLeftChild().isDerivedTable()) {
            // rename this shit
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

        // may need to nest everything in a rename node
//        if(fromClause)
        // check the join conditions
        NestedLoopJoinNode joinNode = new NestedLoopJoinNode(left, right,
                fromClause.getJoinType(), fromClause.getOnExpression());
        joinNode.prepare();

//        PlanNode rename = new RenameNode(joinNode, fromClause.getResultName());
//        rename.prepare();
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

