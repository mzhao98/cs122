package edu.caltech.nanodb.expressions;
import java.util.Collection;
import java.util.HashMap;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

public class AggregateProcessor implements ExpressionProcessor {

    private int counter = 0;
    /*PlanNode newNode = new HashedGroupAggregateNode(PlanNode subplan,
            List<Expression> groupByExprs, Map<String, FunctionCall> aggregates)*/

    private HashMap<String, FunctionCall> aggregates;

    public AggregateProcessor() {
        this.counter = 0;
    /*PlanNode newNode = new HashedGroupAggregateNode(PlanNode subplan,
            List<Expression> groupByExprs, Map<String, FunctionCall> aggregates)*/

        this.aggregates = new HashMap<String, FunctionCall>();
    }

    public void enter(Expression node){
        counter += 1;
    }

    public HashMap<String, FunctionCall> getAggregates(){
        return aggregates;
    }


    public Expression leave(Expression node){
        if(node instanceof FunctionCall)
        {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                String newName = "aggrfn%d" + counter;
                aggregates.put(newName, call);
                return new ColumnValue(new ColumnName(newName));

            }
        }
        return node;
    }

}

