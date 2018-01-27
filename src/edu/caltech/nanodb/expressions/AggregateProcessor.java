package edu.caltech.nanodb.expressions;
import java.util.Collection;
import java.util.HashMap;

import edu.caltech.nanodb.queryast.SelectClause;
import edu.caltech.nanodb.relations.ColumnInfo;
import edu.caltech.nanodb.relations.Schema;
import edu.caltech.nanodb.relations.SchemaNameException;
import edu.caltech.nanodb.functions.AggregateFunction;
import edu.caltech.nanodb.functions.Function;

// This class handles marking of aggregate functions.
public class AggregateProcessor implements ExpressionProcessor {

    private int counter = 0;

    private HashMap<String, FunctionCall> aggregates;

    public AggregateProcessor() {
        this.counter = 0;

        this.aggregates = new HashMap<String, FunctionCall>();
    }
    // Enter each node and increment the counter to
    // prevent repeated column names.
    public void enter(Expression node){
        counter += 1;
    }

    // Return hashmap of aggregates.
    public HashMap<String, FunctionCall> getAggregates(){
        return aggregates;
    }

    // Leave node and mark each expression that is an aggregate
    // function.
    public Expression leave(Expression node){

        if(node instanceof FunctionCall)
        {
            FunctionCall call = (FunctionCall) node;
            Function f = call.getFunction();
            if (f instanceof AggregateFunction) {
                // Throw illegal argument exception if there is a nested aggregate function.
                if (node.toString().contains("aggrfn")){
                    throw new IllegalArgumentException("Can't have nested Aggregate functions");
                }

                String newName = "aggrfn%d" + counter;
                aggregates.put(newName, call);
                return new ColumnValue(new ColumnName(newName));

            }
        }
        return node;
    }

}

