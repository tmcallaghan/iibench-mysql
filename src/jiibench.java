import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.PreparedStatement;
import java.sql.Types;
import java.sql.Timestamp;
import java.sql.Statement;
import java.sql.ResultSet;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Date;
import java.util.Properties;
import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.File;
import java.io.Writer;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class jiibench {
    public static AtomicLong globalInserts = new AtomicLong(0);
    public static AtomicLong globalWriterThreads = new AtomicLong(0);
    public static AtomicLong globalQueryThreads = new AtomicLong(0);
    public static AtomicLong globalQueriesExecuted = new AtomicLong(0);
    public static AtomicLong globalQueriesTimeMs = new AtomicLong(0);
    public static AtomicLong globalQueriesStarted = new AtomicLong(0);
    public static AtomicLong globalInsertExceptions = new AtomicLong(0);

    public static Writer writer = null;
    public static boolean outputHeader = true;

    public static int numCashRegisters = 1000;
    public static int numProducts = 10000;
    public static int numCustomers = 100000;
    public static double maxPrice = 500.0;

    public static String dbName;
    public static int writerThreads;
    public static Integer numMaxInserts;
    public static int rowsPerInsert;
    public static long insertsPerFeedback;
    public static long secondsPerFeedback;
    public static String logFileName;
    public static String indexTechnology;
    public static Long numSeconds;
    public static Integer queriesPerInterval;
    public static Integer queryIntervalSeconds;
    public static double queriesPerMinute;
    public static Long msBetweenQueries;
    public static Integer queryLimit;
    public static Integer queryBeginNumRows;
    public static Integer maxInsertsPerSecond;
    public static Integer maxThreadInsertsPerSecond;
    public static int numCharFields;
    public static int lengthCharFields;
    public static int numSecondaryIndexes;
    public static int percentCompressible;
    public static int numCompressibleCharacters;
    public static int numUncompressibleCharacters;

    public static int randomStringLength = 4*1024*1024;
    public static String randomStringHolder;
    public static int compressibleStringLength =  4*1024*1024;
    public static String compressibleStringHolder;

    public static String tableName = "purchases_index";

    public static String mysqlPort;
    public static String storageEngine;
    public static int innodbKeyBlockSize;
    public static String mysqlServer;
    public static String mysqlUsername;
    public static String mysqlPassword;
    public static String createTable;

    public static int allDone = 0;

    public jiibench() {
    }

    public static void main (String[] args) throws Exception {
        if (args.length != 24) {
            logMe("*** ERROR : CONFIGURATION ISSUE ***");
            logMe("jiibench [database name] [number of writer threads] [rows per table] [rows per insert] [inserts feedback] "+
                      "[seconds feedback] [log file name] [number of seconds to run] [queries per interval] [interval (seconds)] "+
                      "[query limit] [inserts for begin query] [max inserts per second] [num char fields] [length char fields] "+
                      "[num secondary indexes] [percent compressible] [mysql port] [mysql storage engine] [innodb key block size, 0 if none] "+
                      "[mysql server] [mysql username] [mysql password] [create table Y/N]");
            System.exit(1);
        }

        dbName = args[0];
        writerThreads = Integer.valueOf(args[1]);
        numMaxInserts = Integer.valueOf(args[2]);
        rowsPerInsert = Integer.valueOf(args[3]);
        insertsPerFeedback = Long.valueOf(args[4]);
        secondsPerFeedback = Long.valueOf(args[5]);
        logFileName = args[6];
        numSeconds = Long.valueOf(args[7]);
        queriesPerInterval = Integer.valueOf(args[8]);
        queryIntervalSeconds = Integer.valueOf(args[9]);
        queryLimit = Integer.valueOf(args[10]);
        queryBeginNumRows = Integer.valueOf(args[11]);
        maxInsertsPerSecond = Integer.valueOf(args[12]);
        numCharFields = Integer.valueOf(args[13]);
        lengthCharFields = Integer.valueOf(args[14]);
        numSecondaryIndexes = Integer.valueOf(args[15]);
        percentCompressible = Integer.valueOf(args[16]);
        mysqlPort = args[17];
        storageEngine = args[18].toLowerCase();
        innodbKeyBlockSize = Integer.valueOf(args[19]);
        mysqlServer = args[20];
        mysqlUsername = args[21];
        mysqlPassword = args[22];
        createTable = args[23].toLowerCase();

        maxThreadInsertsPerSecond = (maxInsertsPerSecond / writerThreads);

        if ((numSecondaryIndexes < 0) || (numSecondaryIndexes > 3)) {
            logMe("*** ERROR : INVALID NUMBER OF SECONDARY INDEXES, MUST BE >= 0 and <= 3 ***");
            logMe("  %d secondary indexes is not supported",numSecondaryIndexes);
            System.exit(1);
        }

        if ((innodbKeyBlockSize < 0) || (innodbKeyBlockSize > 16)) {
            logMe("*** ERROR : INVALID INNODB KEY BLOCK SIZE, MUST BE >= 0 and <= 16 ***");
            logMe("  innodb key block size of %d is not supported",numSecondaryIndexes);
            System.exit(1);
        }

        if ((queriesPerInterval <= 0) || (queryIntervalSeconds <= 0))
        {
            queriesPerMinute = 0.0;
            msBetweenQueries = 0l;
        }
        else
        {
            queriesPerMinute = (double)queriesPerInterval * (60.0 / (double)queryIntervalSeconds);
            msBetweenQueries = (long)((1000.0 * (double)queryIntervalSeconds) / (double)queriesPerInterval);
        }

        if ((percentCompressible < 0) || (percentCompressible > 100)) {
            logMe("*** ERROR : INVALID PERCENT COMPRESSIBLE, MUST BE >=0 and <= 100 ***");
            logMe("  %d secondary indexes is not supported",percentCompressible);
            System.exit(1);
        }

        numCompressibleCharacters = (int) (((double) percentCompressible / 100.0) * (double) lengthCharFields);
        numUncompressibleCharacters = (int) (((100.0 - (double) percentCompressible) / 100.0) * (double) lengthCharFields);

        logMe("Application Parameters");
        logMe("--------------------------------------------------");
        logMe("  database name = %s",dbName);
        logMe("  %d writer thread(s)",writerThreads);
        logMe("  %,d rows per table",numMaxInserts);
        logMe("  %d character fields",numCharFields);
        if (numCharFields > 0)
        {
            logMe("    %d bytes per character field",lengthCharFields);
            logMe("    %d percent compressible",percentCompressible);
        }
        logMe("  %d secondary indexes",numSecondaryIndexes);
        logMe("  Rows Per Insert = %d",rowsPerInsert);
        logMe("  Maximum of %,d insert(s) per second",maxInsertsPerSecond);
        if (writerThreads > 0)
        {
            logMe("    Maximum of %,d insert(s) per second per writer thread",maxThreadInsertsPerSecond);
        }
        logMe("  Feedback every %,d seconds(s)",secondsPerFeedback);
        logMe("  Feedback every %,d inserts(s)",insertsPerFeedback);
        logMe("  logging to file %s",logFileName);
        logMe("  Run for %,d second(s)",numSeconds);
        if (queriesPerMinute > 0.0)
        {
            logMe("  Attempting %,.2f queries per minute",queriesPerMinute);
            logMe("  Queries limited to %,d row(s)",queryLimit);
            logMe("  Starting queries after %,d row(s) inserted",queryBeginNumRows);
        }
        else
        {
            logMe("  NO queries, insert only benchmark");
        }
        logMe("  MySQL Server= %s",mysqlServer);
        logMe("  MySQL Port= %s",mysqlPort);
        logMe("  MySQL Username= %s",mysqlUsername);
        logMe("  MySQL Storage Engine= %s",storageEngine);
        if (storageEngine.equals("innodb") && (innodbKeyBlockSize > 0))
        {
            logMe("  InnoDB Key Block Size = %d",innodbKeyBlockSize);
        }
        logMe("--------------------------------------------------");

        try {
            // The newInstance() call is a work around for some broken Java implementations
            Class.forName("com.mysql.jdbc.Driver").newInstance();
        } catch (Exception ex) {
            // handle the error
            ex.printStackTrace();
        }

        if (writerThreads > 1) {
            numMaxInserts = numMaxInserts / writerThreads;
        }

        try {
            writer = new BufferedWriter(new FileWriter(new File(logFileName)));
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (createTable.equals("n"))
        {
            logMe("Skipping table creation");
        }
        else
        {
            // create the table
            Connection conn = null;
            Statement stmt = null;

            try {
                conn = DriverManager.getConnection("jdbc:mysql://"+mysqlServer+":"+mysqlPort+"/"+dbName+"?user="+mysqlUsername+"&password="+mysqlPassword+"&rewriteBatchedStatements=true");
                stmt = conn.createStatement();

                // drop the table if it exists
                String sqlDrop = "drop table if exists " + tableName;
                stmt.executeUpdate(sqlDrop);

                // create the table
                String sqlCharFields = "";
                for (int i = 1; i <= numCharFields; i++) {
                    sqlCharFields += " charfield" + Integer.toString(i) + " varchar(" + lengthCharFields + ") not null, ";
                }

                // innodb compression
                String sqlKeyBlockSize = "";
                if ((innodbKeyBlockSize > 0) && storageEngine.equals("innodb")) {
                    sqlKeyBlockSize += " ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=" + Integer.toString(innodbKeyBlockSize) + " ";
                }

                String sqlCreate = "create table " + tableName + " (" +
                                   "transactionid int not null auto_increment, " +
                                   "dateandtime datetime, " +
                                   "cashregisterid int not null, " +
                                   "customerid int not null, " +
                                   "productid int not null, " +
                                   "price float not null, " +
                                   sqlCharFields +
                                   "primary key (transactionid) " +
                                   ") engine=" + storageEngine + sqlKeyBlockSize;
                stmt.executeUpdate(sqlCreate);

                if (numSecondaryIndexes >= 1) {
                    logMe(" *** creating secondary index marketsegment (price, customerid)");
                    String sqlIndex1 = "create index marketsegment on " + tableName + " (price, customerid)";
                    stmt.executeUpdate(sqlIndex1);
                }
                if (numSecondaryIndexes >= 2) {
                    logMe(" *** creating secondary index registersegment (cashregisterid, price, customerid)");
                    String sqlIndex2 = "create index registersegment on " + tableName + " (cashregisterid, price, customerid)";
                    stmt.executeUpdate(sqlIndex2);
                }
                if (numSecondaryIndexes >= 3) {
                    logMe(" *** creating secondary index pdc (price, dateandtime, customerid)");
                    String sqlIndex3 = "create index pdc on " + tableName + " (price, dateandtime, customerid)";
                    stmt.executeUpdate(sqlIndex3);
                }

            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }
        }

        java.util.Random rand = new java.util.Random();

        // create random string holder
        logMe("  creating %,d bytes of random character data...",randomStringLength);
        char[] tempString = new char[randomStringLength];
        for (int i = 0 ; i < randomStringLength ; i++) {
            tempString[i] = (char) (rand.nextInt(26) + 'a');
        }
        randomStringHolder = new String(tempString);

        // create compressible string holder
        logMe("  creating %,d bytes of compressible character data...",compressibleStringLength);
        char[] tempStringCompressible = new char[compressibleStringLength];
        for (int i = 0 ; i < compressibleStringLength ; i++) {
            tempStringCompressible[i] = 'a';
        }
        compressibleStringHolder = new String(tempStringCompressible);


        jiibench t = new jiibench();

        Thread reporterThread = new Thread(t.new MyReporter());
        reporterThread.start();

        Thread queryThread = new Thread(t.new MyQuery(1, 1, numMaxInserts));
        if (queriesPerMinute > 0.0) {
            queryThread.start();
        }

        Thread[] tWriterThreads = new Thread[writerThreads];

        // start the loaders
        for (int i=0; i<writerThreads; i++) {
            globalWriterThreads.incrementAndGet();
            tWriterThreads[i] = new Thread(t.new MyWriter(writerThreads, i, numMaxInserts, maxThreadInsertsPerSecond));
            tWriterThreads[i].start();
        }

        try {
            Thread.sleep(10000);
        } catch (Exception e) {
            e.printStackTrace();
        }

        // wait for reporter thread to terminate
        if (reporterThread.isAlive())
            reporterThread.join();

        // wait for query thread to terminate
        if (queryThread.isAlive())
            queryThread.join();

        // wait for writer threads to terminate
        for (int i=0; i<writerThreads; i++) {
            if (tWriterThreads[i].isAlive())
                tWriterThreads[i].join();
        }

        try {
            if (writer != null) {
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        logMe("Done!");
    }

    class MyWriter implements Runnable {
        int threadCount;
        int threadNumber;
        int numMaxInserts;
        int maxInsertsPerSecond;

        java.util.Random rand;

        MyWriter(int threadCount, int threadNumber, int numMaxInserts, int maxInsertsPerSecond) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.numMaxInserts = numMaxInserts;
            this.maxInsertsPerSecond = maxInsertsPerSecond;
            rand = new java.util.Random((long) threadNumber);
        }
        public void run() {
            Connection conn = null;
            PreparedStatement pstmt = null;

            String sqlCharFields = "";
            String sqlCharFieldPlaceHolders = "";
            for (int i = 1; i <= numCharFields; i++) {
                sqlCharFields += ",charfield" + Integer.toString(i);
                sqlCharFieldPlaceHolders += ",?";
            }

            try {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:"+mysqlPort+"/"+dbName+"?user=root&password=&rewriteBatchedStatements=true");
                pstmt = conn.prepareStatement("INSERT INTO "+tableName+"(dateandtime,cashregisterid,customerid,productid,price"+sqlCharFields+") VALUES (?,?,?,?,?"+sqlCharFieldPlaceHolders+")");
            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }

            long numInserts = 0;
            long numLastInserts = 0;
            int id = 0;
            long nextMs = System.currentTimeMillis() + 1000;

            try {
                logMe("Writer thread %d : started to load table %s",threadNumber, tableName);

                int numRounds = numMaxInserts / rowsPerInsert;

                for (int roundNum = 0; roundNum < numRounds; roundNum++) {
                    if ((numInserts - numLastInserts) >= maxInsertsPerSecond) {
                        // pause until a second has passed
                        while (System.currentTimeMillis() < nextMs) {
                            try {
                                Thread.sleep(20);
                            } catch (Exception e) {
                                e.printStackTrace();
                            }
                        }
                        numLastInserts = numInserts;
                        nextMs = System.currentTimeMillis() + 1000;
                    }

                    Timestamp tsBatch = new Timestamp(System.currentTimeMillis());

                    for (int i = 0; i < rowsPerInsert; i++) {
                        //id++;
                        int thisCustomerId = rand.nextInt(numCustomers);
                        double thisPrice= ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0;

                        pstmt.setTimestamp(1, tsBatch);
                        pstmt.setInt(2, rand.nextInt(numCashRegisters));
                        pstmt.setInt(3, thisCustomerId);
                        pstmt.setInt(4, rand.nextInt(numProducts));
                        pstmt.setDouble(5, thisPrice);

                        for (int charField = 1; charField <= numCharFields; charField++) {
                            int startPosition = rand.nextInt(randomStringLength-lengthCharFields);
                            //doc.put("cf"+Integer.toString(charField), randomStringHolder.substring(startPosition,startPosition+numUncompressibleCharacters) + compressibleStringHolder.substring(startPosition,startPosition+numCompressibleCharacters));
                            pstmt.setString(5+charField, randomStringHolder.substring(startPosition,startPosition+numUncompressibleCharacters) + compressibleStringHolder.substring(startPosition,startPosition+numCompressibleCharacters));
                        }

                        pstmt.addBatch();
                    }

                    try {
                        pstmt.executeBatch();
                        //conn.commit();
                        numInserts += rowsPerInsert;
                        globalInserts.addAndGet(rowsPerInsert);

                    } catch (Exception e) {
                        logMe("Writer thread %d : EXCEPTION",threadNumber);
                        e.printStackTrace();
                        globalInsertExceptions.incrementAndGet();
                    }

                    if (allDone == 1)
                        break;
                }

            } catch (Exception e) {
                logMe("Writer thread %d : EXCEPTION",threadNumber);
                e.printStackTrace();
            }

            long numWriters = globalWriterThreads.decrementAndGet();
            if (numWriters == 0)
                allDone = 1;
        }
    }


    class MyQuery implements Runnable {
        int threadCount;
        int threadNumber;
        int numMaxInserts;

        java.util.Random rand;

        MyQuery(int threadCount, int threadNumber, int numMaxInserts) {
            this.threadCount = threadCount;
            this.threadNumber = threadNumber;
            this.numMaxInserts = numMaxInserts;
            rand = new java.util.Random((long) threadNumber);
        }
        public void run() {
            Connection conn = null;
            PreparedStatement pstmt1 = null;
            PreparedStatement pstmt2 = null;
            PreparedStatement pstmt3 = null;
            PreparedStatement pstmt4 = null;

            String sqlCharFields = "";
            String sqlCharFieldPlaceHolders = "";
            for (int i = 1; i <= numCharFields; i++) {
                sqlCharFields += ",charfield" + Integer.toString(i);
                sqlCharFieldPlaceHolders += ",?";
            }

            try {
                conn = DriverManager.getConnection("jdbc:mysql://localhost:"+mysqlPort+"/"+dbName+"?user=root&password=&rewriteBatchedStatements=true");

                // prepare the 4 possible queries
                pstmt1 = conn.prepareStatement("select transactionid from "+tableName+" where (transactionid >= ?) limit ?");

                pstmt2 = conn.prepareStatement("select price,dateandtime,customerid from "+tableName+" FORCE INDEX (pdc) where " +
                                                    "(price=? and dateandtime=? and customerid>=?) OR " +
                                                    "(price=? and dateandtime>?) OR " +
                                                    "(price>?) LIMIT ?");

                pstmt3 = conn.prepareStatement("select price,customerid from "+tableName+" FORCE INDEX (marketsegment) where " +
                                                    "(price=? and customerid>=?) OR " +
                                                    "(price>?) LIMIT ?");

                pstmt4 = conn.prepareStatement("select cashregisterid,price,customerid from "+tableName+" FORCE INDEX (registersegment) where " +
                                                    "(cashregisterid=? and price=? and customerid>=?) OR " +
                                                    "(cashregisterid=? and price>?) OR " +
                                                    "(cashregisterid>?) LIMIT ?");

            } catch (SQLException ex) {
                // handle any errors
                System.out.println("SQLException: " + ex.getMessage());
                System.out.println("SQLState: " + ex.getSQLState());
                System.out.println("VendorError: " + ex.getErrorCode());
            }

            long t0 = System.currentTimeMillis();
            long lastMs = t0;
            long nextQueryMillis = t0;
            boolean outputWaiting = true;
            boolean outputStarted = true;

            long numQueriesExecuted = 0;
            long numQueriesTimeMs = 0;

            int whichQuery = 0;

            try {
                logMe("Query thread %d : ready to query table %s",threadNumber, tableName);

                while (allDone == 0) {
                    //try {
                    //    Thread.sleep(10);
                    //} catch (Exception e) {
                    //    e.printStackTrace();
                    // }

                    long thisNow = System.currentTimeMillis();

                    // wait until my next runtime
                    if (thisNow > nextQueryMillis) {
                        nextQueryMillis = thisNow + msBetweenQueries;

                        // check if number of inserts reached
                        if (globalInserts.get() >= queryBeginNumRows) {
                            if (outputStarted)
                            {
                                logMe("Query thread %d : now running",threadNumber,queryBeginNumRows);
                                outputStarted = false;
                                // set query start time
                                globalQueriesStarted.set(thisNow);
                            }

                            whichQuery++;
                            if (whichQuery > 4) {
                                whichQuery = 1;
                            }

                            long thisRandomTransactionId = rand.nextLong() % globalInserts.get();
                            int thisCustomerId = rand.nextInt(numCustomers);
                            double thisPrice = ((rand.nextDouble() * maxPrice) + (double) thisCustomerId) / 100.0;
                            int thisCashRegisterId = rand.nextInt(numCashRegisters);
                            int thisProductId = rand.nextInt(numProducts);
                            long thisRandomTime = t0 + (long) ((double) (thisNow - t0) * rand.nextDouble());
                            Timestamp thisRandomTimestamp = new Timestamp(thisRandomTime);

                            long now = System.currentTimeMillis();
                            if (whichQuery == 1) {
                                pstmt1.setLong(1, thisRandomTransactionId);
                                pstmt1.setInt(2, queryLimit);
                                ResultSet rs = pstmt1.executeQuery();
                                /*
                                int count = 0;
                                while (rs.next()) {
                                    ++count;
                                }
                                logMe("Query %d : %,d row(s)",whichQuery,count);
                                */
                            } else if (whichQuery == 2) {
                                pstmt2.setDouble(1, thisPrice);
                                pstmt2.setTimestamp(2, thisRandomTimestamp);
                                pstmt2.setInt(3, thisCustomerId);
                                pstmt2.setDouble(4, thisPrice);
                                pstmt2.setTimestamp(5, thisRandomTimestamp);
                                pstmt2.setDouble(6, thisPrice);
                                pstmt2.setInt(7, queryLimit);
                                ResultSet rs = pstmt2.executeQuery();
                                /*
                                int count = 0;
                                while (rs.next()) {
                                    ++count;
                                }
                                logMe("Query %d : %,d row(s)",whichQuery,count);
                                */
                            } else if (whichQuery == 3) {
                                pstmt3.setDouble(1, thisPrice);
                                pstmt3.setInt(2, thisCustomerId);
                                pstmt3.setDouble(3, thisPrice);
                                pstmt3.setInt(4, queryLimit);
                                ResultSet rs = pstmt3.executeQuery();
                                /*
                                int count = 0;
                                while (rs.next()) {
                                    ++count;
                                }
                                logMe("Query %d : %,d row(s)",whichQuery,count);
                                */
                            } else if (whichQuery == 4) {
                                pstmt4.setInt(1, thisCashRegisterId);
                                pstmt4.setDouble(2, thisPrice);
                                pstmt4.setInt(3, thisCustomerId);
                                pstmt4.setInt(4, thisCashRegisterId);
                                pstmt4.setDouble(5, thisPrice);
                                pstmt4.setInt(6, thisCashRegisterId);
                                pstmt4.setInt(7, queryLimit);
                                ResultSet rs = pstmt4.executeQuery();
                                /*
                                int count = 0;
                                while (rs.next()) {
                                    ++count;
                                }
                                logMe("Query %d : %,d row(s)",whichQuery,count);
                                */
                            }
                            long elapsed = System.currentTimeMillis() - now;

                            globalQueriesExecuted.incrementAndGet();
                            globalQueriesTimeMs.addAndGet(elapsed);
                        } else {
                            if (outputWaiting)
                            {
                                logMe("Query thread %d : waiting for %,d row insert(s) before starting",threadNumber,queryBeginNumRows);
                                outputWaiting = false;
                            }
                        }
                    }
                }

            } catch (Exception e) {
                logMe("Query thread %d : EXCEPTION",threadNumber);
                e.printStackTrace();
            }

            long numQueries = globalQueryThreads.decrementAndGet();
        }
    }


    // reporting thread, outputs information to console and file
    class MyReporter implements Runnable {
        public void run()
        {
            long t0 = System.currentTimeMillis();
            long lastInserts = 0;
            long lastQueriesNum = 0;
            long lastQueriesMs = 0;
            long lastMs = t0;
            long intervalNumber = 0;
            long nextFeedbackMillis = t0 + (1000 * secondsPerFeedback * (intervalNumber + 1));
            long nextFeedbackInserts = lastInserts + insertsPerFeedback;
            long thisInserts = 0;
            long thisQueriesNum = 0;
            long thisQueriesMs = 0;
            long thisQueriesStarted = 0;
            long endDueToTime = System.currentTimeMillis() + (1000 * numSeconds);

            while (allDone == 0)
            {
                try {
                    Thread.sleep(100);
                } catch (Exception e) {
                    e.printStackTrace();
                }

                long now = System.currentTimeMillis();

                if (now >= endDueToTime)
                {
                    allDone = 1;
                }

                thisInserts = globalInserts.get();
                thisQueriesNum = globalQueriesExecuted.get();
                thisQueriesMs = globalQueriesTimeMs.get();
                thisQueriesStarted = globalQueriesStarted.get();
                if (((now > nextFeedbackMillis) && (secondsPerFeedback > 0)) ||
                    ((thisInserts >= nextFeedbackInserts) && (insertsPerFeedback > 0)))
                {
                    intervalNumber++;
                    nextFeedbackMillis = t0 + (1000 * secondsPerFeedback * (intervalNumber + 1));
                    nextFeedbackInserts = (intervalNumber + 1) * insertsPerFeedback;

                    long elapsed = now - t0;
                    long thisIntervalMs = now - lastMs;

                    long thisIntervalInserts = thisInserts - lastInserts;
                    double thisIntervalInsertsPerSecond = thisIntervalInserts/(double)thisIntervalMs*1000.0;
                    double thisInsertsPerSecond = thisInserts/(double)elapsed*1000.0;

                    long thisIntervalQueriesNum = thisQueriesNum - lastQueriesNum;
                    long thisIntervalQueriesMs = thisQueriesMs - lastQueriesMs;
                    double thisIntervalQueryAvgMs = 0;
                    double thisQueryAvgMs = 0;
                    double thisIntervalAvgQPM = 0;
                    double thisAvgQPM = 0;

                    long thisInsertExceptions = globalInsertExceptions.get();

                    if (thisIntervalQueriesNum > 0) {
                        thisIntervalQueryAvgMs = thisIntervalQueriesMs/(double)thisIntervalQueriesNum;
                    }
                    if (thisQueriesNum > 0) {
                        thisQueryAvgMs = thisQueriesMs/(double)thisQueriesNum;
                    }

                    if (thisQueriesStarted > 0)
                    {
                        long adjustedElapsed = now - thisQueriesStarted;
                        if (adjustedElapsed > 0)
                        {
                            thisAvgQPM = (double)thisQueriesNum/((double)adjustedElapsed/1000.0/60.0);
                        }
                        if (thisIntervalMs > 0)
                        {
                            thisIntervalAvgQPM = (double)thisIntervalQueriesNum/((double)thisIntervalMs/1000.0/60.0);
                        }
                    }

                    if (secondsPerFeedback > 0)
                    {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qpm=%,.2f : int avg qpm=%,.2f : exceptions=%,d", thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM, thisInsertExceptions);
                    } else {
                        logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qpm=%,.2f : int avg qpm=%,.2f : exceptions=%,d", intervalNumber * insertsPerFeedback, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM, thisInsertExceptions);
                    }

                    try {
                        if (outputHeader)
                        {
                            writer.write("tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qpm\tint_qpm\texceptions\n");
                            outputHeader = false;
                        }

                        String statusUpdate = "";

                        if (secondsPerFeedback > 0)
                        {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM, thisInsertExceptions);
                        } else {
                            statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%,d\n",intervalNumber * insertsPerFeedback, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM, thisInsertExceptions);
                        }
                        writer.write(statusUpdate);
                        writer.flush();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }

                    lastInserts = thisInserts;
                    lastQueriesNum = thisQueriesNum;
                    lastQueriesMs = thisQueriesMs;

                    lastMs = now;
                }
            }

            // output final numbers...
            long now = System.currentTimeMillis();
            thisInserts = globalInserts.get();
            thisQueriesNum = globalQueriesExecuted.get();
            thisQueriesMs = globalQueriesTimeMs.get();
            thisQueriesStarted = globalQueriesStarted.get();
            intervalNumber++;
            nextFeedbackMillis = t0 + (1000 * secondsPerFeedback * (intervalNumber + 1));
            nextFeedbackInserts = (intervalNumber + 1) * insertsPerFeedback;
            long elapsed = now - t0;
            long thisIntervalMs = now - lastMs;
            long thisIntervalInserts = thisInserts - lastInserts;
            double thisIntervalInsertsPerSecond = thisIntervalInserts/(double)thisIntervalMs*1000.0;
            double thisInsertsPerSecond = thisInserts/(double)elapsed*1000.0;
            long thisIntervalQueriesNum = thisQueriesNum - lastQueriesNum;
            long thisIntervalQueriesMs = thisQueriesMs - lastQueriesMs;
            double thisIntervalQueryAvgMs = 0;
            double thisQueryAvgMs = 0;
            double thisIntervalAvgQPM = 0;
            double thisAvgQPM = 0;
            if (thisIntervalQueriesNum > 0) {
                thisIntervalQueryAvgMs = thisIntervalQueriesMs/(double)thisIntervalQueriesNum;
            }
            if (thisQueriesNum > 0) {
                thisQueryAvgMs = thisQueriesMs/(double)thisQueriesNum;
            }
            if (thisQueriesStarted > 0)
            {
                long adjustedElapsed = now - thisQueriesStarted;
                if (adjustedElapsed > 0)
                {
                    thisAvgQPM = (double)thisQueriesNum/((double)adjustedElapsed/1000.0/60.0);
                }
                if (thisIntervalMs > 0)
                {
                    thisIntervalAvgQPM = (double)thisIntervalQueriesNum/((double)thisIntervalMs/1000.0/60.0);
                }
            }
            if (secondsPerFeedback > 0)
            {
                logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qpm=%,.2f : int avg qpm=%,.2f", thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM);
            } else {
                logMe("%,d inserts : %,d seconds : cum ips=%,.2f : int ips=%,.2f : cum avg qry=%,.2f : int avg qry=%,.2f : cum avg qpm=%,.2f : int avg qpm=%,.2f", intervalNumber * insertsPerFeedback, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM);
            }
            try {
                if (outputHeader)
                {
                    writer.write("tot_inserts\telap_secs\tcum_ips\tint_ips\tcum_qry_avg\tint_qry_avg\tcum_qpm\tint_qpm\n");
                    outputHeader = false;
                }
                String statusUpdate = "";
                if (secondsPerFeedback > 0)
                {
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",thisInserts, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM);
                } else {
                    statusUpdate = String.format("%d\t%d\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\t%.2f\n",intervalNumber * insertsPerFeedback, elapsed / 1000l, thisInsertsPerSecond, thisIntervalInsertsPerSecond, thisQueryAvgMs, thisIntervalQueryAvgMs, thisAvgQPM, thisIntervalAvgQPM);
                }
                writer.write(statusUpdate);
                writer.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }

        }
    }


    public static void logMe(String format, Object... args) {
        System.out.println(Thread.currentThread() + String.format(format, args));
    }
}
