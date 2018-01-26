package edu.caltech.test.nanodb.sql;


import org.testng.annotations.Test;

import edu.caltech.nanodb.expressions.TupleLiteral;
import edu.caltech.nanodb.server.CommandResult;
import edu.caltech.nanodb.server.NanoDBServer;


/**
 * This class exercises the database with some simple <tt>JOIN</tt>
 * statements against a single table, to see if simple selects and
 * predicates work properly.
 */
@Test
public class TestSimpleJoins extends SqlTestCase {

    public TestSimpleJoins() {
        super("setup_testSimpleJoins");
    }


    /**
     * This test performs a simple <tt>JOIN</tt> statement with no predicate,
     * to see if the query produces the expected results.
     *
     * @throws Exception if any query parsing or execution issues occur.
     */
    public void testInnerJoinEN() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
            "SELECT * FROM test_simple_joins_c JOIN test_simple_joins_a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testInnerJoinNE() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b JOIN test_simple_joins_d", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testInnerJoinEE() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_c JOIN test_simple_joins_d", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testInnerJoinM1() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 2, 3, 2, 3, 1),
                new TupleLiteral(1, 2, 3, 3, 4, 2),
                new TupleLiteral(1, 2, 3, 3, 3, 3),
                new TupleLiteral(2, 2, 3, 2, 3, 1),
                new TupleLiteral(2, 2, 3, 3, 4, 2),
                new TupleLiteral(2, 2, 3, 3, 3, 3),
                new TupleLiteral(2, 3, 1, 2, 3, 1),
                new TupleLiteral(2, 3, 1, 3, 4, 2),
                new TupleLiteral(2, 3, 1, 3, 3, 3)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_a JOIN test_simple_joins_b", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testInnerJoin1M() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(2, 3, 1, 1, 2, 3),
                new TupleLiteral(2, 3, 1, 2, 2, 3),
                new TupleLiteral(2, 3, 1, 2, 3, 1),
                new TupleLiteral(3, 4, 2, 1, 2, 3),
                new TupleLiteral(3, 4, 2, 2, 2, 3),
                new TupleLiteral(3, 4, 2, 2, 3, 1),
                new TupleLiteral(3, 3, 3, 1, 2, 3),
                new TupleLiteral(3, 3, 3, 2, 2, 3),
                new TupleLiteral(3, 3, 3, 2, 3, 1)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b JOIN test_simple_joins_a", true);
        assert checkUnorderedResults(expected, result);
    }


    
    

    public void testLeftOuterJoinEN() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_c LEFT OUTER JOIN test_simple_joins_a ON test_simple_joins_c.a = test_simple_joins_a.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testLeftOuterJoinNE() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(2, 3, 1, null, null, null),
                new TupleLiteral(3, 4, 2, null, null, null),
                new TupleLiteral(3, 3, 3, null, null, null)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b LEFT OUTER JOIN test_simple_joins_d ON test_simple_joins_b.a = test_simple_joins_d.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testLeftOuterJoinEE() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_c LEFT OUTER JOIN test_simple_joins_d ON test_simple_joins_c.a = test_simple_joins_d.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testLeftOuterJoinM1() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(1, 2, 3, null, null, null),
                new TupleLiteral(2, 2, 3, 2, 3, 1),
                new TupleLiteral(2, 3, 1, 2, 3, 1)

        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_a LEFT OUTER JOIN test_simple_joins_b ON test_simple_joins_a.a = test_simple_joins_b.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testLeftOuterJoin1M() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(2, 3, 1, 2, 2, 3),
                new TupleLiteral(2, 3, 1, 2, 3, 1),
                new TupleLiteral(3, 4, 2, null, null, null),
                new TupleLiteral(3, 3, 3, null, null, null)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b LEFT OUTER JOIN test_simple_joins_a ON test_simple_joins_b.a = test_simple_joins_a.a", true);
        assert checkUnorderedResults(expected, result);
    }







    public void testRightOuterJoinEN() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(null, null, 1, 2, 3),
                new TupleLiteral(null, null, 2, 2, 3),
                new TupleLiteral(null, null, 2, 3, 1)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_c RIGHT OUTER JOIN test_simple_joins_a ON test_simple_joins_c.a = test_simple_joins_a.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testRightOuterJoinNE() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b RIGHT OUTER JOIN test_simple_joins_d ON test_simple_joins_b.a = test_simple_joins_d.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testRightOuterJoinEE() throws Throwable {
        TupleLiteral[] expected = {
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_c RIGHT OUTER JOIN test_simple_joins_d ON test_simple_joins_c.a = test_simple_joins_d.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testRightOuterJoinM1() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(2, 2, 3, 2, 3, 1),
                new TupleLiteral(2, 3, 1, 2, 3, 1),
                new TupleLiteral(null, null, null, 3, 4, 2),
                new TupleLiteral(null, null, null, 3, 3, 3)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_a RIGHT OUTER JOIN test_simple_joins_b ON test_simple_joins_a.a = test_simple_joins_b.a", true);
        assert checkUnorderedResults(expected, result);
    }

    public void testRightOuterJoin1M() throws Throwable {
        TupleLiteral[] expected = {
                new TupleLiteral(null, null, null, 1, 2, 3),
                new TupleLiteral(2, 3, 1, 2, 2, 3),
                new TupleLiteral(2, 3, 1, 2, 3, 1)
        };

        CommandResult result = server.doCommand(
                "SELECT * FROM test_simple_joins_b RIGHT OUTER JOIN test_simple_joins_a ON test_simple_joins_b.a = test_simple_joins_a.a", true);
        assert checkUnorderedResults(expected, result);
    }
}