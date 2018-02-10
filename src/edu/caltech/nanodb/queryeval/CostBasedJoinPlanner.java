package edu.caltech.nanodb.queryeval;


import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import edu.caltech.nanodb.expressions.AggregateProcessor;
import edu.caltech.nanodb.expressions.PredicateUtils;
import edu.caltech.nanodb.plannodes.*;
import edu.caltech.nanodb.queryast.SelectValue;
import edu.caltech.nanodb.relations.JoinType;
import org.apache.log4j.Logger;

import edu.caltech.nanodb.expressions.Expression;
import edu.caltech.nanodb.queryast.FromClause;
import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.TableInfo;


/**
 * This planner implementation uses dynamic programming to devise an optimal
 * join strategy for the query.  As always, queries are optimized in units of
 * <tt>SELECT</tt>-<tt>FROM</tt>-<tt>WHERE</tt> subqueries; optimizations
 * don't currently span multiple subqueries.
 */
public class CostBasedJoinPlanner extends AbstractPlannerImpl {

    /** A logging object for reporting anything interesting that happens. */
    private static Logger logger = Logger.getLogger(CostBasedJoinPlanner.class);

    /**
     * This helper class is used to keep track of one "join component" in the
     * dynamic programming algorithm.  A join component is simply a query plan
     * for joining one or more leaves of the query.
     * <p>
     * In this context, a "leaf" may either be a base table or a subquery in
     * the <tt>FROM</tt>-clause of the query.  However, the planner will
     * attempt to push conjuncts down the plan as far as possible, so even if
     * a leaf is a base table, the plan may be a bit more complex than just a
     * single file-scan.
     */
    private static class JoinComponent {
        /**
         * This is the join plan itself, that joins together all leaves
         * specified in the {@link #leavesUsed} field.
         */
        public PlanNode joinPlan;

        /**
         * This field specifies the collection of leaf-plans that are joined by
         * the plan in this join-component.
         */
        public HashSet<PlanNode> leavesUsed;

        /**
         * This field specifies the collection of all conjuncts use by this join
         * plan.  It allows us to easily determine what join conjuncts still
         * remain to be incorporated into the query.
         */
        public HashSet<Expression> conjunctsUsed;

        /**
         * Constructs a new instance for a <em>leaf node</em>.  It should not
         * be used for join-plans that join together two or more leaves.  This
         * constructor simply adds the leaf-plan into the {@link #leavesUsed}
         * collection.
         *
         * @param leafPlan the query plan for this leaf of the query.
         *
         * @param conjunctsUsed the set of conjuncts used by the leaf plan.
         *        This may be an empty set if no conjuncts apply solely to
         *        this leaf, or it may be nonempty if some conjuncts apply
         *        solely to this leaf.
         */
        public JoinComponent(PlanNode leafPlan, HashSet<Expression> conjunctsUsed) {
            leavesUsed = new HashSet<>();
            leavesUsed.add(leafPlan);

            joinPlan = leafPlan;

            this.conjunctsUsed = conjunctsUsed;
        }

        /**
         * Constructs a new instance for a <em>non-leaf node</em>.  It should
         * not be used for leaf plans!
         *
         * @param joinPlan the query plan that joins together all leaves
         *        specified in the <tt>leavesUsed</tt> argument.
         *
         * @param leavesUsed the set of two or more leaf plans that are joined
         *        together by the join plan.
         *
         * @param conjunctsUsed the set of conjuncts used by the join plan.
         *        Obviously, it is expected that all conjuncts specified here
         *        can actually be evaluated against the join plan.
         */
        public JoinComponent(PlanNode joinPlan, HashSet<PlanNode> leavesUsed,
                             HashSet<Expression> conjunctsUsed) {
            this.joinPlan = joinPlan;
            this.leavesUsed = leavesUsed;
            this.conjunctsUsed = conjunctsUsed;
        }
    }


    /**
     * Returns the root of a plan tree suitable for executing the specified
     * query.
     *
     * @param selClause an object describing the query to be performed
     *
     * @return a plan tree for executing the specified query
     *
     * @throws java.io.IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     */
    public PlanNode makePlan(SelectClause selClause,
        List<SelectClause> enclosingSelects) throws IOException {

        // How this function works:
        //
        // 1)  Pull out the top-level conjuncts from the WHERE and HAVING
        //     clauses on the query, since we will handle them in special ways
        //     if we have outer joins.
        //
        // 2)  Create an optimal join plan from the top-level from-clause and
        //     the top-level conjuncts.
        //
        // 3)  If there are any unused conjuncts, determine how to handle them.
        //
        // 4)  Create a project plan-node if necessary.
        //
        // 5)  Handle other clauses such as ORDER BY, LIMIT/OFFSET, etc.
        //
        // Supporting other query features, such as grouping/aggregation,
        // various kinds of subqueries, queries without a FROM clause, etc.,
        // has been incorporated into this sketch.


        // how to do the where clause
        // how to find and what to do with unused conjuncts
        // handling project plan nodes
        // handling other cases the same way

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

        HashSet<Expression> whereConjuncts = new HashSet<>();

        // first we get the details from where and having
        if(selClause.getWhereExpr() != null)
        {
            // check if aggregates in the where clause
            Expression onEx = selClause.getWhereExpr().traverse(processor);
            if (onEx.toString().contains("aggrfn")) {
                throw new IllegalArgumentException("WHERE clause can't have aggregate functions");
            }

            PredicateUtils.collectConjuncts(selClause.getWhereExpr(), whereConjuncts);
        }

        PlanNode plan;

        // now make the join plan

        // logger.warn("make join plan");
        JoinComponent joinComponent = makeJoinPlan(fromClause, whereConjuncts);
        // logger.warn(String.format("made join plan"));
        // PredicateUtils.collectConjuncts(fromClause);


        // get the unused conjuncts - see if there is a better way to do this

        // logger.warn(String.format("joining plan"));

        plan = joinComponent.joinPlan;

        // handles the unused conjuncts in the where clause
        // logger.warn("got plan");

        // logger.warn(String.format("whereConjuncts before: %s", whereConjuncts.toString()));
        whereConjuncts.removeAll(joinComponent.conjunctsUsed);
        // logger.warn(String.format("whereConjuncts after: %s", whereConjuncts.toString()));


        // logger.warn(String.format("conjuncts used %s", joinComponent.conjunctsUsed));
        // logger.warn("got where conjuncts");
        if(!whereConjuncts.isEmpty())
        {
            // logger.warn("where not empty");
            Expression pred = PredicateUtils.makePredicate(whereConjuncts);

            // logger.warn(String.format("adding predicates %s", pred));

            PlanUtils.addPredicateToPlan(plan, pred);
            // logger.warn(String.format("added schema %s", plan.getSchema().toString()));
            plan.prepare();
        }
        // logger.warn("after where");

        // grouping and aggregation
        if(hasAgg || selClause.getGroupByExprs().size() > 0) {

            PlanNode aggNode = plan;
            plan = new HashedGroupAggregateNode(aggNode,
                    selClause.getGroupByExprs(), processor.getAggregates());
            plan.prepare();
        }

        // then make the projects
        if(!selClause.isTrivialProject())
        {
            // logger.warn("project");
            PlanNode oldPlan = plan;
            plan = new ProjectNode(oldPlan, selClause.getSelectValues());
            plan.prepare();
            // logger.warn(String.format("projected %s", oldPlan.getSchema()));
        }

        // now we order by
        if(selClause.getOrderByExprs().size() > 0)
        {
            // logger.warn("order by");
            PlanNode sNode = plan;
            plan = new SortNode(sNode, selClause.getOrderByExprs());
            plan.prepare();
        }

        // logger.warn(String.format("returning %s", plan.toString()));

        return plan;
    }


    /**
     * Given the top-level {@code FromClause} for a SELECT-FROM-WHERE block,
     * this helper generates an optimal join plan for the {@code FromClause}.
     *
     * @param fromClause the top-level {@code FromClause} of a
     *        SELECT-FROM-WHERE block.
     * @param extraConjuncts any extra conjuncts (e.g. from the WHERE clause,
     *        or HAVING clause)
     * @return a {@code JoinComponent} object that represents the optimal plan
     *         corresponding to the FROM-clause
     * @throws IOException if an IO error occurs during planning.
     */
    private JoinComponent makeJoinPlan(FromClause fromClause,
        Collection<Expression> extraConjuncts) throws IOException {

        // These variables receive the leaf-clauses and join conjuncts found
        // from scanning the sub-clauses.  Initially, we put the extra conjuncts
        // into the collection of conjuncts.
        HashSet<Expression> conjuncts = new HashSet<>();
        ArrayList<FromClause> leafFromClauses = new ArrayList<>();

        collectDetails(fromClause, conjuncts, leafFromClauses);

        logger.debug("Making join-plan for " + fromClause);
        logger.debug("    Collected conjuncts:  " + conjuncts);
        logger.debug("    Collected FROM-clauses:  " + leafFromClauses);
        logger.debug("    Extra conjuncts:  " + extraConjuncts);

        if (extraConjuncts != null)
            conjuncts.addAll(extraConjuncts);

        // Make a read-only set of the input conjuncts, to avoid bugs due to
        // unintended side-effects.
        Set<Expression> roConjuncts = Collections.unmodifiableSet(conjuncts);

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        logger.debug("Generating plans for all leaves");
        ArrayList<JoinComponent> leafComponents = generateLeafJoinComponents(
            leafFromClauses, roConjuncts);

        // Print out the results, for debugging purposes.
        if (logger.isDebugEnabled()) {
            for (JoinComponent leaf : leafComponents) {
                logger.debug("    Leaf plan:\n" +
                    PlanNode.printNodeTreeToString(leaf.joinPlan, true));
            }
        }

        // Build up the full query-plan using a dynamic programming approach.

        JoinComponent optimalJoin =
            generateOptimalJoin(leafComponents, roConjuncts);

        PlanNode plan = optimalJoin.joinPlan;
        logger.info("Optimal join plan generated:\n" +
            PlanNode.printNodeTreeToString(plan, true));

        return optimalJoin;
    }


    /**
     * This helper method pulls the essential details for join optimization
     * out of a <tt>FROM</tt> clause.
     *
     * This function recursively calls on the fromClause to populate
     * the conjuncts HashSet and the leafFromClauses ArrayList. This method
     * considers base-tables, sub-queries, and outer-joins to be leaves. It only
     * collects conjuncts from predicates that appear in non-leaf FromClauses,
     * which signify inner joins.
     *
     * @param fromClause the from-clause to collect details from
     *
     * @param conjuncts the collection to add all conjuncts to
     *
     * @param leafFromClauses the collection to add all leaf from-clauses to
     */
    private void collectDetails(FromClause fromClause,
        HashSet<Expression> conjuncts, ArrayList<FromClause> leafFromClauses) {

        // If fromClause is a base table, this is our break condition, add it
        // to leafFromClauses and return.
        if (fromClause.isBaseTable()){
            leafFromClauses.add(fromClause);
            return;
        }
        // If fromClause is a derived table, add it to leafFromClauses and recursively
        // call collectDetails on the selectClause.
        if (fromClause.isDerivedTable()) {
            leafFromClauses.add(fromClause);
            // collectDetails(fromClause.getSelectClause().getFromClause(), conjuncts, leafFromClauses);

        }
        // If fromClause is an outer join table, add it to leafFromClauses and recursively
        // call collectDetails on the left and right children.
        else if(fromClause.isOuterJoin()) {
            leafFromClauses.add(fromClause);
            // collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
            // collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
        }
        // Else if, fromClause is an inner join table, collect conjuncts from the predicates
        // that appear in this non-leaf fromClause and recursively
        // call collectDetails on the left and right children.
        else{
            if(fromClause.isJoinExpr()){
                PredicateUtils.collectConjuncts(fromClause.getOnExpression(), conjuncts);
                collectDetails(fromClause.getLeftChild(), conjuncts, leafFromClauses);
                collectDetails(fromClause.getRightChild(), conjuncts, leafFromClauses);
            }
        }
    }


    /**
     * This helper method performs the first step of the dynamic programming
     * process to generate an optimal join plan, by generating a plan for every
     * leaf from-clause identified from analyzing the query.  Leaf plans are
     * usually very simple; they are built either from base-tables or
     * <tt>SELECT</tt> subqueries.  The most complex detail is that any
     * conjuncts in the query that can be evaluated solely against a particular
     * leaf plan-node will be associated with the plan node.  <em>This is a
     * heuristic</em> that usually produces good plans (and certainly will for
     * the current state of the database), but could easily interfere with
     * indexes or other plan optimizations.
     *
     * @param leafFromClauses the collection of from-clauses found in the query
     *
     * @param conjuncts the collection of conjuncts that can be applied at this
     *                  level
     *
     * @return a collection of {@link JoinComponent} object containing the plans
     *         and other details for each leaf from-clause
     *
     * @throws IOException if a particular database table couldn't be opened or
     *         schema loaded, for some reason
     */
    private ArrayList<JoinComponent> generateLeafJoinComponents(
        Collection<FromClause> leafFromClauses, Collection<Expression> conjuncts)
        throws IOException {

        // Create a subplan for every single leaf FROM-clause, and prepare the
        // leaf-plan.

        ArrayList<JoinComponent> leafComponents = new ArrayList<>();
        for (FromClause leafClause : leafFromClauses) {
            HashSet<Expression> leafConjuncts = new HashSet<>();

            PlanNode leafPlan =
                makeLeafPlan(leafClause, conjuncts, leafConjuncts);

            JoinComponent leaf = new JoinComponent(leafPlan, leafConjuncts);
            leafComponents.add(leaf);
        }
        return leafComponents;
    }


    /**
     * Constructs a plan tree for evaluating the specified from-clause.
     *
     * This method constructs a plan tree for leaf nodes given
     * leaf from clauses. Taking the nodes that need to be joined, it
     * determines the type of node that requires a plan, and given this
     * node, either recursively calls the planner, recursively joins
     * the children, or returns a base table.
     *
     * @param fromClause the select nodes that need to be joined.
     *
     * @param conjuncts additional conjuncts that can be applied when
     *        constructing the from-clause plan.
     *
     * @param leafConjuncts this is an output-parameter.  Any conjuncts
     *        applied in this plan from the <tt>conjuncts</tt> collection
     *        should be added to this out-param.
     *
     * @return a plan tree for evaluating the specified from-clause
     *
     * @throws IOException if an IO error occurs when the planner attempts to
     *         load schema and indexing information.
     *
     * @throws IllegalArgumentException if the specified from-clause is a join
     *         expression that isn't an outer join, or has some other
     *         unrecognized type.
     */
    private PlanNode makeLeafPlan(FromClause fromClause,
        Collection<Expression> conjuncts, HashSet<Expression> leafConjuncts)
        throws IOException {

        //        If you apply any conjuncts then make sure to add them to the
        //        leafConjuncts collection.
        //
        //        Don't forget that all from-clauses can specify an alias.
        //
        //        Concentrate on properly handling cases other than outer
        //        joins first, then focus on outer joins once you have the
        //        typical cases supported.

        // logger.warn("Making leaf\n\n\n");

        // figure out if we need to rename
        PlanNode leafPlan = null;

        if(fromClause.isBaseTable())
        {
            // Expression predicate = PredicateUtils.makePredicate(conjuncts);
            // predicate will be applied at bottom
            PlanNode temp = makeSimpleSelect(fromClause.getTableName(), null, null);
            // logger.warn("base table");
            if(fromClause.getResultName() != null && !fromClause.getResultName().equals(fromClause.getTableName())) {
                leafPlan = new RenameNode(temp, fromClause.getResultName());
                // logger.warn(String.format("renamed, %s", fromClause.getResultName()));

            }
            else
            {
                leafPlan = temp;
            }

        }

        else if(fromClause.isDerivedTable())
        {
            // call make leaf plan
            // leafPlan = makePlan(fromClause.getSelectClause(), null);

            // logger.warn("derived");
            PlanNode child = makePlan(fromClause.getSelectClause(), null);
            leafPlan = new RenameNode(child, fromClause.getResultName());
            // logger.warn("end recurse");

        }

        else if(fromClause.isOuterJoin())
        {
            // first, test equivalency on left:
            PlanNode join = null;
            // logger.warn("OUTER JOIN");

            if(fromClause.hasOuterJoinOnLeft())
            {
                // logger.warn("OUTER JOIN L");


                // we can only pass down stuff on the left
                HashSet<Expression> left = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, left,
                        fromClause.getLeftChild().getSchema());

                // logger.warn(String.format("got left %s", left));

                HashSet<Expression> right = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, right,
                        fromClause.getRightChild().getSchema());

                // logger.warn(String.format("left child %s", fromClause.getLeftChild()));

                // pass down stuff on left
                JoinComponent jc1 = makeJoinPlan(fromClause.getLeftChild(), left);

                // logger.warn(String.format("right child %s", fromClause.getRightChild()));

                // don't pass down stuff on right
                JoinComponent jc2 = makeJoinPlan(fromClause.getRightChild(), null);


                // logger.warn("made JOIN plans");

                HashSet<Expression> rightleft = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, rightleft,
                        fromClause.getRightChild().getSchema(), fromClause.getLeftChild().getSchema());

                // logger.warn(String.format("got rightleft %s", rightleft));
                rightleft.removeAll(right);
                rightleft.removeAll(left);

                Expression pred = PredicateUtils.makePredicate(rightleft);

                // logger.warn(String.format("got preddd %s", rightleft));
                // logger.warn(String.format("On expr pred %s", fromClause.getOnExpression()));

                // join on the predicate if it corresponds to left and right
                // join = new JoinComponent(leafPlan, comps, rightleft);

                join = new NestedLoopJoinNode(jc1.joinPlan, jc2.joinPlan, fromClause.getJoinType(), fromClause.getOnExpression());
                // logger.warn(String.format("Join %s", join));


            }

            else if(fromClause.hasOuterJoinOnRight())
            {

                HashSet<Expression> left = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, left,
                        fromClause.getLeftChild().getSchema());

                // logger.warn(String.format("got left %s", left));

                HashSet<Expression> right = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, right,
                        fromClause.getRightChild().getSchema());

                // logger.warn(String.format("left child %s", fromClause.getLeftChild()));

                // don't pass down stuff on left
                JoinComponent jc1 = makeJoinPlan(fromClause.getLeftChild(), null);


                // logger.warn(String.format("right child %s", fromClause.getRightChild()));

                // pass down stuff on right
                JoinComponent jc2 = makeJoinPlan(fromClause.getRightChild(), right);


                // logger.warn("made JOIN plans");

                HashSet<Expression> rightleft = new HashSet<>();
                PredicateUtils.findExprsUsingSchemas(conjuncts, false, rightleft,
                        fromClause.getRightChild().getSchema(), fromClause.getLeftChild().getSchema());

                // logger.warn(String.format("got rightleft %s", rightleft));
                rightleft.removeAll(right);
                rightleft.removeAll(left);

                // use remaining expression for predicate
                Expression pred = PredicateUtils.makePredicate(rightleft);

                // logger.warn(String.format("got preddd %s", rightleft));
                // logger.warn(String.format("On expr pred %s", fromClause.getOnExpression()));


                join = new NestedLoopJoinNode(jc1.joinPlan, jc2.joinPlan, fromClause.getJoinType(), fromClause.getOnExpression());
                // logger.warn(String.format("Join %s", join));

            }

            leafPlan = join;

        }

        // For some reason I thought this method could be called with inner and
        // cross joins. It can't. Thus we don't need the following code.
        // I left it
//        else
//        {
//            // otherwise inner/cross join
//            PlanNode join = null;
//
//            // logger.warn("cross JOIN\n\n\n");
//
//            HashSet<Expression> left = new HashSet<>();
//            PredicateUtils.findExprsUsingSchemas(conjuncts, false, left,
//                    fromClause.getLeftChild().getSchema());
//
//            HashSet<Expression> right = new HashSet<>();
//            PredicateUtils.findExprsUsingSchemas(conjuncts, false, right,
//                    fromClause.getRightChild().getSchema());
//
//            // pass down corresponding expressions to both sides
//            JoinComponent jc1 = makeJoinPlan(fromClause.getLeftChild(), left);
//            JoinComponent jc2 = makeJoinPlan(fromClause.getRightChild(), right);
//
//            HashSet<Expression> rightleft = new HashSet<>();
//            PredicateUtils.findExprsUsingSchemas(conjuncts, false, rightleft,
//                    fromClause.getRightChild().getSchema(), fromClause.getLeftChild().getSchema());
//
//            rightleft.removeAll(right);
//            rightleft.removeAll(left);
//
//            // use remaining expression for predicate
//            Expression pred = PredicateUtils.makePredicate(rightleft);
//
//            // join the left and right components
//            join = new NestedLoopJoinNode(jc1.joinPlan, jc2.joinPlan, fromClause.getJoinType(), pred);
//
//
//            leafPlan = join;
//
////            if(fromClause.getResultName() != null) {
////                leafPlan = new RenameNode(join, fromClause.getResultName());
////            }
////            else
////            {
////                leafPlan = join;
////            }
//        }

        leafPlan.prepare();

        // logger.warn(String.format("before pred, schema = %s", leafPlan.getSchema().toString()));

        HashSet<Expression> used = new HashSet<>();
        PredicateUtils.findExprsUsingSchemas(conjuncts, false, used,
                leafPlan.getSchema());

        // logger.warn(String.format("asdfsd", conjuncts.toString()));
        Expression pred = PredicateUtils.makePredicate(used);

        if(pred != null) {
            // logger.warn(String.format("got pred, %s", pred.toString()));

            PlanUtils.addPredicateToPlan(leafPlan, pred);
            leafPlan.prepare();

        }
        // logger.warn(String.format("added to plan %s", leafPlan.toString()));

        return leafPlan;
    }


    /**
     * This helper method builds up a full join-plan using a dynamic programming
     * approach.  The implementation maintains a collection of optimal
     * intermediate plans that join <em>n</em> of the leaf nodes, each with its
     * own associated cost, and then uses that collection to generate a new
     * collection of optimal intermediate plans that join <em>n+1</em> of the
     * leaf nodes.  This process completes when all leaf plans are joined
     * together; there will be <em>one</em> plan, and it will be the optimal
     * join plan (as far as our limited estimates can determine, anyway).
     *
     * @param leafComponents the collection of leaf join-components, generated
     *        by the {@link #generateLeafJoinComponents} method.
     *
     * @param conjuncts the collection of all conjuncts found in the query
     *
     * @return a single {@link JoinComponent} object that joins all leaf
     *         components together in an optimal way.
     */
    private JoinComponent generateOptimalJoin(
        ArrayList<JoinComponent> leafComponents, Set<Expression> conjuncts) {

        // This object maps a collection of leaf-plans (represented as a
        // hash-set) to the optimal join-plan for that collection of leaf plans.
        //
        // This collection starts out only containing the leaf plans themselves,
        // and on each iteration of the loop below, join-plans are grown by one
        // leaf.  For example:
        //   * In the first iteration, all plans joining 2 leaves are created.
        //   * In the second iteration, all plans joining 3 leaves are created.
        //   * etc.
        // At the end, the collection will contain ONE entry, which is the
        // optimal way to join all N leaves.  Go Go Gadget Dynamic Programming!
        HashMap<HashSet<PlanNode>, JoinComponent> joinPlans = new HashMap<>();

        // Initially populate joinPlans with just the N leaf plans.
        for (JoinComponent leaf : leafComponents)
            joinPlans.put(leaf.leavesUsed, leaf);

        while (joinPlans.size() > 1) {
            logger.debug("Current set of join-plans has " + joinPlans.size() +
                " plans in it.");

            // This is the set of "next plans" we will generate.  Plans only
            // get stored if they are the first plan that joins together the
            // specified leaves, or if they are better than the current plan.
            HashMap<HashSet<PlanNode>, JoinComponent> nextJoinPlans =
                new HashMap<>();

            //        JOIN N + 1 LEAVES
            // Iterate over plans that join n leaves.
            for (JoinComponent plan: joinPlans.values()){
                // Iterate over the leaf plans.
                for (JoinComponent leaf : leafComponents){
                    // If leaf already appears in plan n, continue.
                    // logger.warn(String.format("plan %s", plan.joinPlan.getSchema()));
                    // logger.warn(String.format("leaf %s", leaf.joinPlan.getSchema()));

                    if (plan.leavesUsed.contains(leaf.joinPlan) || plan.joinPlan.equals(leaf.joinPlan)){
                        continue;
                    }

                    // logger.warn(String.format("plan %s", plan.leavesUsed));
                    // logger.warn(String.format("leaf %s", leaf.joinPlan));

                    // Instantiate a new join plan that joins together plan n and leaf.
                    // First compute the sub-plan conjuncts by set unions of plan and leaf child conjuncts.
                    HashSet<Expression> leftConjuncts = plan.conjunctsUsed;
                    HashSet<Expression> rightConjuncts = leaf.conjunctsUsed;
                    // logger.warn(String.format("got conjuncts"));

                    HashSet<Expression> subplanConjuncts = new HashSet<Expression>(leftConjuncts);
                    subplanConjuncts.addAll(rightConjuncts);

                    // logger.warn(String.format("got sugb conjuncts"));
                    // Second, compute the unused conjuncts by set difference of all conjuncts and sub-plan conjuncts.
                    HashSet<Expression> unusedConjuncts = new HashSet<Expression>(conjuncts);
                    unusedConjuncts.removeAll(subplanConjuncts);

                    // logger.warn(String.format("removed"));

                    // Third, determine which of those conjuncts should be applied to the theta join.
                    HashSet<Expression> finalConjuncts = new HashSet<Expression>();

                    // logger.warn(String.format("getting conjuncts to use %s", unusedConjuncts));
                    PredicateUtils.findExprsUsingSchemas(unusedConjuncts,
                            false, finalConjuncts, plan.joinPlan.getSchema(), leaf.joinPlan.getSchema());

                    // logger.warn(String.format("found exprs"));

                    // logger.warn(String.format("final conjuncts are %s", finalConjuncts));

                    // Fourth, create a new predicate using the final conjuncts.
                    Expression newPredicate = PredicateUtils.makePredicate(finalConjuncts);

                    // logger.warn(String.format("made pred %s", newPredicate));

                    // Fifth, create a nested loop join node to hold the new join and new predicate.
                    // logger.warn(String.format("nested loop join leaf %s", leaf.joinPlan.toString()));
                    // logger.warn(String.format("nested loop join plan %s", plan.joinPlan.toString()));

                    PlanNode newPlans = new NestedLoopJoinNode(plan.joinPlan, leaf.joinPlan, JoinType.INNER, newPredicate);
                    newPlans.prepare();

                    // Compute the cost of the new plan.
                    float newCost = newPlans.getCost().cpuCost;


                    HashSet<PlanNode> combinedLeaves = new HashSet<PlanNode>(plan.leavesUsed);
                    combinedLeaves.addAll(leaf.leavesUsed);

                    JoinComponent newPlanComponent = new JoinComponent(newPlans,
                            combinedLeaves, finalConjuncts);

                    // logger.warn(String.format("\n\n\nnew plan %s", newPlanComponent.conjunctsUsed   ));
                    // logger.warn(String.format("new cost %f", newCost));


                    // If nextJoinPlans already contains a plan with all leaves in nextJoinPlans,
                    // compare the cost of the new plan to the current best plan in nextJoinPlans.

                    if(nextJoinPlans.containsKey(combinedLeaves)){
                        if (newCost < nextJoinPlans.get(combinedLeaves).joinPlan.getCost().cpuCost){
                            nextJoinPlans.replace(combinedLeaves, newPlanComponent);
                        }
                    }
                    // If nextJoinPlans already contains a plan with all leaves in nextJoinPlans,
                    // add the new plan to nextJoinPlans.
                    else{
                        nextJoinPlans.put(combinedLeaves, newPlanComponent);
                    }


                }
            }



            // Now that we have generated all plans joining N leaves, time to
            // create all plans joining N + 1 leaves.
            joinPlans = nextJoinPlans;
        }

        // At this point, the set of join plans should only contain one plan,
        // and it should be the optimal plan.

        assert joinPlans.size() == 1 : "There can be only one optimal join plan!";
        return joinPlans.values().iterator().next();
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
