/*
 * Derby - Class org.apache.derbyTesting.functionTests.tests.store.IndexSplitDeadlockTest
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific
 * language governing permissions and limitations under the License.
 */

package org.apache.derbyTesting.functionTests.tests.store;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import junit.framework.Test;
import org.apache.derbyTesting.junit.BaseJDBCTestCase;
import org.apache.derbyTesting.junit.CleanDatabaseTestSetup;
import org.apache.derbyTesting.junit.DatabasePropertyTestSetup;
import org.apache.derbyTesting.junit.JDBC;
import org.apache.derbyTesting.junit.TestConfiguration;

/**
 * Test that executes the code paths changed by the fix for the index split
 * deadlock (DERBY-2991). The main purpose is to test that index scans are
 * able to reposition in cases where they release the latch on the leaf page
 * on which they are positioned (typically because they had to wait for a
 * lock, or because they returned control to the caller after fetching a
 * bulk of rows).
 */
public class IndexSplitDeadlockTest extends BaseJDBCTestCase {

    /**
     * List of threads (AsyncThread objects) to wait for after running the test.
     */
    private List threads = new ArrayList();

    public IndexSplitDeadlockTest(String name) {
        super(name);
    }

    public static Test suite() {
        Test test = TestConfiguration.embeddedSuite(
                IndexSplitDeadlockTest.class);
        test = new CleanDatabaseTestSetup(test);
        test = DatabasePropertyTestSetup.setLockTimeouts(test, 2, 4);
        return test;
    }

    protected void tearDown() throws Exception {
        rollback();
        setAutoCommit(false); // required by JDBC.dropSchema()
        JDBC.dropSchema(getConnection().getMetaData(), "APP");

        // Go through all the threads and call waitFor() so that we
        // detect errors that happened in another thread.
        for (Iterator it = threads.iterator(); it.hasNext();) {
            AsyncThread thread = (AsyncThread) it.next();
            thread.waitFor();
        }
        threads = null;

        super.tearDown();
    }

    // --------------------------------------------------------------------
    // Test cases for bugs related to saving position and repositioning
    // --------------------------------------------------------------------

    /**
     * Test that a max scan works when it needs to wait more than once in order
     * to lock the last record in the index. This used to cause an assert
     * failure in sane builds before DERBY-4193.
     */
    public void testMultipleLastKeyWaitsInMaxScan() throws Exception {
        setAutoCommit(false);

        // Create a table with an index and a couple of rows.
        Statement s = createStatement();
        s.execute("create table max_scan(x int, y int)");
        s.execute("create index idx on max_scan(x)");
        s.execute("insert into max_scan(x) values 1,2,3");
        commit();

        // Start a thread that (1) obtains an exclusive lock on the last
        // row, (2) waits for the main thread to perform a max scan that will
        // be blocked by the lock, (3) inserts values greater than the current
        // max so that the main thread needs to rescan when it wakes up, (4)
        // commit to allow the main thread to continue, and (5) immediately
        // insert more rows greater than the previous max so that the main
        // thread is likely to have to wait for a lock a second time.
        new AsyncThread(new AsyncTask() {
            public void doWork(Connection conn) throws Exception {
                conn.setAutoCommit(false);
                Statement s = conn.createStatement();
                s.execute("update max_scan set y = x where x = 3");
                s.close();

                // Give the main thread time to start executing select max(x)
                // and wait for the lock.
                Thread.sleep(2000);

                // Insert rows greater than the current max.
                PreparedStatement ps = conn.prepareStatement(
                        "insert into max_scan(x) values 4");
                for (int i = 0; i < 300; i++) {
                    ps.execute();
                }

                // Commit and release locks to allow the main thread to
                // continue.
                conn.commit();

                // Insert some more rows so that the main thread is likely to
                // have to wait again. Note that there is a possibility that
                // the main thread manages to obtain the lock on the last row
                // before we manage to insert a new row, in which case it
                // won't have to wait for us and we're not actually testing
                // a max scan that needs to wait more than once to lock the
                // last row.
                for (int i = 0; i < 300; i++) {
                    ps.execute();
                }

                // Block for a while before releasing locks, so that the main
                // thread will have to wait if it didn't obtain the lock on the
                // last row before we did.
                Thread.sleep(500);
                conn.commit();

                ps.close();
            }
        });

        // Give the other thread a little while to start and obtain the
        // lock on the last record.
        Thread.sleep(1000);

        // The last record should be locked now, so this call will have to
        // wait initially. This statement used to cause an assert failure in
        // debug builds before DERBY-4193.
        JDBC.assertSingleValueResultSet(
                s.executeQuery("select max(x) from max_scan " +
                               "--DERBY-PROPERTIES index=IDX"),
                "4");
    }

    /**
     * Test that a forward scan works even in the case that it has to wait
     * for the previous key lock more than once. This used to cause an assert
     * failure in sane builds before DERBY-4193.
     */
//    public void testMultiplePrevKeyWaitsInForwardScan() throws Exception {
//        setAutoCommit(false);

        // Isolation level should be serializable so that the scan needs
        // a previous key lock.
//        getConnection().setTransactionIsolation(
//                Connection.TRANSACTION_SERIALIZABLE);

        // Create a table with an index and a couple of rows.
//        Statement s = createStatement();
//        s.execute("create table fw_scan(x int)");
//        s.execute("create index idx on fw_scan(x)");
//        s.execute("insert into fw_scan(x) values 100,200,300");
//        commit();

 //       new AsyncThread(new AsyncTask() {
//            public void doWork(Connection conn) throws Exception {
//                conn.setAutoCommit(false);
//                PreparedStatement ps =
//                        conn.prepareStatement("insert into fw_scan values 1");

                // Insert one row right before the first row to be returned
                // by the scan. This will be the previous key that the scan
                // will attempt to lock. Wait for two seconds to allow the
                // scan to start and attempt to lock the record.
//                ps.execute();
//                Thread.sleep(2000);

                // Before we commit and release the lock, insert more rows
                // between the locked row and the first row of the scan, so
                // that another row holds the previous key for the scan when
                // it wakes up.
//                for (int i = 0; i < 300; i++) {
//                    ps.execute();
//               }
//                conn.commit();

                // The scan will wake up and try to lock the row that has
                // now become the row immediately to the left of its starting
                // position. Try to beat it to it so that it has to wait a
                // second time in order to obtain the previous key lock. This
                // used to trigger an assert failure in the scan before
                // DERBY-4193.
//                for (int i = 0; i < 300; i++) {
//                    ps.execute();
//               }

                // Wait a little while to give the scan enough time to wake
                // up and make another attempt to lock the previous key before
                // we release the locks.
//                Thread.sleep(500);
//                conn.rollback();
//                ps.close();
//            }
//        });

        // Give the other thread a second to start and obtain a lock that
        // blocks the scan.
//        Thread.sleep(1000);

        // The key to the left of the first key to be returned by the scan
        // should be locked now. This call will have to wait for the previous
        // key lock at least once. If it has to wait a second time (dependent
        // on the exact timing between this thread and the other thread) the
        // assert error from DERBY-4193 will be exposed.
//        JDBC.assertSingleValueResultSet(
//                s.executeQuery("select x from fw_scan " +
//                               "--DERBY-PROPERTIES index=IDX\n" +
//                               "where x >= 100 and x < 200"),
//                "100");
//    }

    // --------------------------------------------------------------------
    // Helpers
    // --------------------------------------------------------------------

    /**
     * <p>
     * In a separate thread, and in a separate transaction, execute the
     * SQL text and wait for the specified period of time, before the
     * transaction is rolled back. This method can be used to hold locks
     * and thereby block the main thread for a certain amount of time.
     * </p>
     *
     * <p>
     * If an exception is thrown while executing the SQL, the exception is
     * stored and rethrown from the tearDown() method in the main execution
     * thread, so that it is detected by the JUnit framework.
     * </p>
     *
     * @param sql the SQL text to execute
     * @param blockMillis how many milliseconds to wait until the transaction
     * is rolled back
     */
    private void obstruct(final String sql, final long blockMillis) {
        AsyncTask task = new AsyncTask() {
            public void doWork(Connection conn) throws Exception {
                conn.setAutoCommit(false);
                Statement s = conn.createStatement();
                s.execute(sql);
                s.close();
                Thread.sleep(blockMillis);
            }
        };
        new AsyncThread(task);
    }

    /**
     * Interface that should be implemented by classes that define a
     * database task that is to be executed asynchronously in a separate
     * transaction.
     */
    private static interface AsyncTask {
        void doWork(Connection conn) throws Exception;
    }

    /**
     * Class that executes an {@code AsyncTask} object.
     */
    private class AsyncThread implements Runnable {

        private final Thread thread = new Thread(this);
        private final AsyncTask task;
        private Exception error;

        /**
         * Create an {@code AsyncThread} object and starts a thread executing
         * the task. Also put the {@code AsyncThread} object in the list of
         * threads in the parent object to make sure the thread is waited for
         * and its errors detected in the {@code tearDown()} method.
         *
         * @param task the task to perform
         */
        public AsyncThread(AsyncTask task) {
            this.task = task;
            thread.start();
            threads.add(this);
        }

        /**
         * Open a database connection and perform the task. Roll back the
         * transaction when finished. Any exception thrown will be caught and
         * rethrown when the {@code waitFor()} method is called.
         */
        public void run() {
            try {
                Connection conn = openDefaultConnection();
                try {
                    task.doWork(conn);
                } finally {
                    JDBC.cleanup(conn);
                }
            } catch (Exception e) {
                error = e;
            }
        }

        /**
         * Wait for the thread to complete. If an error was thrown during
         * execution, rethrow the execption here.
         * @throws Exception if an error happened while performing the task
         */
        void waitFor() throws Exception {
            thread.join();
            if (error != null) {
                throw error;
            }
        }
    }
}
