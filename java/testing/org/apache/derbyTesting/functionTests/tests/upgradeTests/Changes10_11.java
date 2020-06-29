/*

Derby - Class org.apache.derbyTesting.functionTests.tests.upgradeTests.Changes10_11

Licensed to the Apache Software Foundation (ASF) under one or more
contributor license agreements.  See the NOTICE file distributed with
this work for additional information regarding copyright ownership.
The ASF licenses this file to You under the Apache License, Version 2.0
(the "License"); you may not use this file except in compliance with
the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

*/
package org.apache.derbyTesting.functionTests.tests.upgradeTests;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;


/**
 * Upgrade test cases for 10.11.
 */
public class Changes10_11 extends UpgradeChange
{

    //////////////////////////////////////////////////////////////////
    //
    // CONSTANTS
    //
    //////////////////////////////////////////////////////////////////

    private static  final   String  SYNTAX_ERROR = "42X01";
    private static  final   String  HARD_UPGRADE_REQUIRED = "XCL47";
    private static  final   String  NOT_IMPLEMENTED = "0A000";
    private static  final   String  NO_ROWS_AFFECTED = "02000";
    private static  final   String  UNKNOWN_OPTIONAL_TOOL = "X0Y88";
    private static  final   String  UNRECOGNIZED_PROCEDURE = "42Y03";

    //////////////////////////////////////////////////////////////////
    //
    // CONSTRUCTOR
    //
    //////////////////////////////////////////////////////////////////

    public Changes10_11(String name) {
        super(name);
    }

    //////////////////////////////////////////////////////////////////
    //
    // JUnit BEHAVIOR
    //
    //////////////////////////////////////////////////////////////////

    /**
     * Return the suite of tests to test the changes made in 10.11.
     *
     * @param phase an integer that indicates the current phase in
     *              the upgrade test.
     * @return the test suite created.
     */
    public static Test suite(int phase) {
        return new TestSuite(Changes10_11.class, "Upgrade test for 10.11");
    }

    //////////////////////////////////////////////////////////////////
    //
    // TESTS
    //
    //////////////////////////////////////////////////////////////////
/** Test that identity columns handle self-deadlock in soft-upgrade mode */
    public void test_derby6692() throws Exception
    {
        Connection  conn = getConnection();

        switch (getPhase())
        {
            case PH_CREATE:
            case PH_SOFT_UPGRADE:
            case PH_POST_SOFT_UPGRADE:
            case PH_HARD_UPGRADE:

                boolean originalAutoCommit = conn.getAutoCommit();
                try
                {
                    conn.setAutoCommit( false );

                    conn.prepareStatement( "create table t_6692(i int generated always as identity)" ).execute();
                    conn.prepareStatement( "insert into t_6692 values (default)" ).execute();

                    conn.rollback();
                }
                finally
                {
                    conn.setAutoCommit( originalAutoCommit );
                }
                break;
        }
    }   


 private int countSequences( Statement statement )
        throws Exception
    {
        return count( statement, "select count(*) from sys.syssequences" );
    }
    private int count( Statement statement, String query ) throws Exception
    {
        ResultSet   rs = statement.executeQuery( query );
        rs.next();

        try {
            return rs.getInt( 1 );
        }
        finally
        {
            rs.close();
        }
    }

    /** Return the boolean value of a system property */
    private static  boolean getBooleanProperty( Properties properties, String key )
    {
        return Boolean.valueOf( properties.getProperty( key ) ).booleanValue();
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    private void    expectExecutionWarning( Connection conn, String sqlState, String query )
        throws Exception
    {
        expectExecutionWarnings( conn, new String[] { sqlState }, query );
    }

    /**
     * Assert that the statement text, when executed, raises a warning.
     */
    private void    expectExecutionWarnings( Connection conn, String[] sqlStates, String query )
        throws Exception
    {
        println( "\nExpecting warnings " + fill( sqlStates ).toString() + " when executing:\n\t"  );
        PreparedStatement   ps = chattyPrepare( conn, query );

        ps.execute();

        int idx = 0;

        for ( SQLWarning sqlWarning = ps.getWarnings(); sqlWarning != null; sqlWarning = sqlWarning.getNextWarning() )
        {
            String          actualSQLState = sqlWarning.getSQLState();

            if ( idx >= sqlStates.length )
            {
                fail( "Got more warnings than we expected." );
            }

            String  expectedSqlState = sqlStates[ idx++ ];

            assertEquals( expectedSqlState, actualSQLState );
        }

        assertEquals( idx, sqlStates.length );

        ps.close();
    }

    /**
     * <p>
     * Fill an ArrayList from an array.
     * </p>
     */
    protected <T> ArrayList<T> fill( T[] raw )
    {
        return new ArrayList<T>(Arrays.asList(raw));
    }
}
