package phoenix.multicopy.dataloader;

/**
 * PHOENIX DATA LOADER MULTI COPY - Loads Multiple Copies of Data from MS SQL Server to the HBase Cluster Via Phoenix
 */

/**
 * @author Nithin Vommi
 *
 */

import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang.time.StopWatch;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Phoenix_Multicopy_Dataloader
{
    public static final Logger LOGGER = LoggerFactory
            .getLogger(Phoenix_Multicopy_Dataloader.class);
        
    public static final List<BlockingQueue<ArrayList<HashMap<String,Object>>>> queues = new ArrayList<>(10000);
    
    public static final Map<Class<?>, Parameter> PARAMETERS_MAP = new HashMap<>();
    
    static 
    {
        PARAMETERS_MAP.put(String.class, new StringParameter());
        PARAMETERS_MAP.put(Integer.class, new IntegerParameter());
        PARAMETERS_MAP.put(Long.class, new LongParameter());
        PARAMETERS_MAP.put(Short.class, new ShortParameter());
        PARAMETERS_MAP.put(Byte.class, new ByteParameter());
        PARAMETERS_MAP.put(Double.class, new DoubleParameter());
        PARAMETERS_MAP.put(Timestamp.class, new TimestampParameter());
        PARAMETERS_MAP.put(UUID.class, new UUIDParameter());
        PARAMETERS_MAP.put(Date.class, new DateParameter());
        PARAMETERS_MAP.put(Boolean.class, new BooleanParameter());
        PARAMETERS_MAP.put(byte[].class, new ByteArrayParameter());
    }
    
    public static void main (String[] args)
    {
    	
    	//CLI Arguments--------------------------------------------------
    	
    	String sqlserver_ip 		= args[0];
    	String sqlserver_port 		= args[1];
    	String sqlserver_db 		= args[2];
    	String sqlserver_username 	= args[3];
    	String sqlserver_password 	= args[4];
    	String phoenix_ip 			= args[5];
    	int Insertthreads 			= Integer.parseInt(args[6]);
    	int reusability_factor		= Integer.parseInt(args[7]);
    	int batchsize				= Integer.parseInt(args[8]);
    	String sqltable				= args[9];
    	String phoenixtable			= args[10];
    	int offset					= Integer.parseInt(args[11]);
    	int maxrows					= (Insertthreads * reusability_factor * batchsize) + offset;
    	
    	
        //--------------------------------------------------------------
    	

        
        BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue = new ArrayBlockingQueue<>(20000);
        DBReadThread reader = new DBReadThread(1, blockingQueue,maxrows, sqltable, offset, 
        		phoenixtable, batchsize, sqlserver_ip, sqlserver_port, sqlserver_db, sqlserver_username, sqlserver_password);
        reader.run();
        queues.add(reader.getReadBlockingQueue());

        ExecutorService executor = Executors.newFixedThreadPool(Insertthreads);
        
        StopWatch watch = new StopWatch();
        System.out.println("Timer Started");
        watch.start();
        
        for (int i = 0; i < (Insertthreads*reusability_factor); i++) 
        {
            InsertThread thread;
            try 
            {
                thread = new InsertThread(i, queues.get(0), reader.getResultSetMetaData(), reader.getprep_template(), phoenix_ip);
                LOGGER.info("Submitting Thread ID - " + (i + 1) + "/" + (Insertthreads*reusability_factor));
                executor.submit(thread);
            } 
            
            catch (Exception e) 
            {
                System.out.println("Exception" +  e);
                executor.shutdownNow();
                System.exit(-1);
                return;
            }
            
        }
        LOGGER.info("Executor Service Shutdown Triggered");
        executor.shutdown();
        
        while(!executor.isTerminated())
        {
//            System.out.println("Awaiting Completion : " + System.currentTimeMillis());
        }
        
        long timeTaken = watch.getTime();
        LOGGER.info("Total Insert Time taken = " +  timeTaken);

    }
    
    
    public static class DBReadThread extends Thread 
    {
        public final int thread_id;
        public final BlockingQueue<ArrayList<HashMap<String,Object>>> ReadblockingQueue;
        public final int MAXROWS;
        public final String SQLTABLE;
        public final int OFFSET;
        public final String PHOENIXTABLE;
        public final int BATCHSIZE;
        public final String SQL_IP;
        public final String SQL_PORT;
        public final String SQL_DB;
        public final String user;
        public final String password;
        

        ArrayList<HashMap<String,Object>> list = new ArrayList<>();
        Statement sqlServerStmt = null;
        ResultSet sqlServerResultSet = null;
        ResultSetMetaData md = null;
        String prep_template = null;
        
        //Constructor
        public DBReadThread(int i, BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue, int maxrows, String sqltable, int offset, 
        		String phoenixtable, int batchsize, String sqlserver_ip, String sqlserver_port, String sqlserver_db, String user, String password)
        {
            this.thread_id = i;
            this.ReadblockingQueue = blockingQueue;
            this.MAXROWS = maxrows;
            this.SQLTABLE = sqltable;
            this.OFFSET = offset;
            this.PHOENIXTABLE = phoenixtable;
            this.BATCHSIZE = batchsize;
            this.SQL_IP = sqlserver_ip;
            this.SQL_PORT = sqlserver_port;
            this.SQL_DB = sqlserver_db;
            this.user = user;
            this.password = password;
            
        }
        
        public BlockingQueue<ArrayList<HashMap<String,Object>>> getReadBlockingQueue()
        {

            return this.ReadblockingQueue;
        }
        
        public ResultSetMetaData getResultSetMetaData()
        {
            return md;
        }
        
        public String getprep_template()
        {
            return prep_template;
        }
        
        @Override
        public void run()
        {
            
            //Connection
            Connection sqlServerConnection = null;
            sqlServerConnection = sqlconnect(SQL_IP, SQL_PORT, SQL_DB, user, password);
            

            //Query Records
            try
            {
                sqlServerStmt = sqlServerConnection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                sqlServerStmt.setMaxRows(MAXROWS);
                String query = "SELECT * FROM EPOEVENTSMT AS A LEFT JOIN EPExtendedEventMT AS B ON A.AUTOID = B.EventAutoID "
                		+ "LEFT JOIN WP_EventInfoMT AS C ON A.AutoID = C.EventAutoID "
                		+ "LEFT JOIN EPCertEventMT AS D ON A.AutoID = D.EventAutoID;";
                sqlServerResultSet = sqlServerStmt.executeQuery(query);
                LOGGER.info(query + " - Query Executed");
                md = sqlServerResultSet.getMetaData();               
            }
            catch (SQLException e)
            {
                LOGGER.error("Unable to execute DBRead Query by Thread : " + thread_id);
                e.printStackTrace();
            }
            
            
            try
            {
                int columns = md.getColumnCount();
                sqlServerResultSet.absolute(OFFSET);
                while (sqlServerResultSet.next()) 
                {
                    
                    StringBuilder queryBuilder = new StringBuilder();
                    StringBuilder valuesBuilder = new StringBuilder();
                    HashMap<String,Object> row = new HashMap<String, Object>(columns);
                    
                    queryBuilder.append("UPSERT INTO ");
                    queryBuilder.append(PHOENIXTABLE);
                    queryBuilder.append(" (");
                                    
                    valuesBuilder.append(" VALUES (");
                    
                    for(int i=1; i<=columns; ++i) 
                    {
                        row.put(md.getTableName(i)+"_"+md.getColumnName(i),sqlServerResultSet.getObject(i));
                        
                        if (md.getTableName(i).equalsIgnoreCase("epoeventsmt"))
                        {
                        	queryBuilder.append(md.getColumnName(i));
                        }
                        else if(md.getColumnName(i).equalsIgnoreCase("eventautoid"))
                        {
                        	queryBuilder.append(md.getTableName(i) + "_ExtEventExist");
                        }
                        else
                        {
                        	queryBuilder.append(md.getTableName(i) + "_" + md.getColumnName(i));
                        }
                        
                        queryBuilder.append(",");
                        
                        valuesBuilder.append("?");
                        valuesBuilder.append(",");
                    }
                    queryBuilder.deleteCharAt(queryBuilder.length() - 1);
                    valuesBuilder.deleteCharAt(valuesBuilder.length() - 1);
                    
                    queryBuilder.append(")");
                    queryBuilder.append(valuesBuilder);
                    queryBuilder.append(")");
                    
                    list.add(row);
                    
                    prep_template = queryBuilder.toString();
                    
                    LOGGER.info("Fetched Row : " + list.size());
                    
                    if (list.size() == BATCHSIZE) 
                    {
                        System.out.println("Inserting into Blocking Queue of thread #" + thread_id);
                        ReadblockingQueue.add(list);
                        list = new ArrayList<HashMap<String,Object>>();
                    } 
                }
                
            }
            catch (Exception e)
            {
                LOGGER.error("Unable to Read Data by Thread : " + thread_id);
                e.printStackTrace();
            }
        }
        
        public Connection sqlconnect(String SQL_IP, String SQL_PORT, String SQL_DB, String user, String password)
        {
            Connection connection = null;
            try
            {
            	try 
            	{
            		Class.forName("net.sourceforge.jtds.jdbc.Driver");
            	} 
            	catch (ClassNotFoundException e) 
            	{
            		System.out.println("SQL Driver Class Not Found");
            		e.printStackTrace();
            	}

            	connection = DriverManager.getConnection("jdbc:jtds:sqlserver://" + SQL_IP + ":" + SQL_PORT + ";" + "databaseName="+ SQL_DB +";"+"user="+ user + ";" + 
            																	"password=" + password + ";"+ "ApplicationIntent=ReadOnly");
                System.out.println("Connection Established : SQL SERVER");
            }
            catch (SQLException e)
            {
                e.printStackTrace();
            }
            return connection;
        }
    }
    
    
    
    public static class InsertThread extends Thread 
    {

        public BlockingQueue<ArrayList<HashMap<String,Object>>> WriteblockingQueue = new ArrayBlockingQueue<ArrayList<HashMap<String,Object>>>(200);
        public ResultSetMetaData md = null;
        public final String PHOENIX_IP;
        public final int thread_id;
        
        public PreparedStatement pstmt = null;
        public String prep_template = null;
        public Connection phoenix_connection = null;
        
        //Constructor
        public InsertThread(int i, BlockingQueue<ArrayList<HashMap<String,Object>>> blockingQueue, ResultSetMetaData table_md, String str, String phoenix_ip)
        {

            this.thread_id = i;
            this.WriteblockingQueue = blockingQueue;
            this.md = table_md;
            this.prep_template = str;
            this.PHOENIX_IP = phoenix_ip;
            
            try
            {
                Class.forName("org.apache.phoenix.jdbc.PhoenixDriver");
//				Class.forName("org.apache.calcite.avatica.remote.Driver");
            }
            catch (Exception e)
            {
                LOGGER.error("Phoenix Driver Not Found");
            }
            
            try 
            {
                phoenix_connection =  DriverManager.getConnection("jdbc:phoenix:"+ PHOENIX_IP +":2181");
//				phoenix_connection = DriverManager.getConnection("jdbc:phoenix:thin:url=http://"+ PHOENIX_IP +":8765;serialization=PROTOBUF");  
                LOGGER.info("THREAD - " + (thread_id + 1) + " Connection Established : PHOENIX");
            }         
            catch (SQLException e) 
            {
                LOGGER.error("THREAD - " + (thread_id + 1) + " Error Establishing Connection with PHOENIX");
                e.printStackTrace();
            }
            
            
            try 
            {
                pstmt = phoenix_connection.prepareStatement(prep_template);
            } 
            catch (SQLException e) 
            {
                LOGGER.error("Unable to create Prepared Statement");
                e.printStackTrace();
            }
        }
        
        @Override
        public void run()
        {
            ArrayList<HashMap<String, Object>> chunk = null;
            
            try 
            {
            	LOGGER.info("THREAD - " + (thread_id + 1) + " Taking Chunk");
                chunk = WriteblockingQueue.take();
            } 
            catch (InterruptedException e) 
            {
                LOGGER.error("Unable to receive job");
                e.printStackTrace();
            }
            
            LOGGER.info("THREAD - " + (thread_id + 1) + " Inserting Chunk");         
            insert(chunk);
            
            try 
            {
                phoenix_connection.commit();
            } 
            
            catch (SQLException e) 
            {
                LOGGER.error("Unable to Commit the Upsert");
                e.printStackTrace();
            }
        }
        
        public void insert(ArrayList<HashMap<String,Object>> piece) 
        {
            int i = 0;
            while (i < piece.size())
            {
                try
                {                
                    for (int column_id = 1; column_id <= md.getColumnCount(); column_id++) 
                    {
                        Parameter p;
                        int coltype = md.getColumnType(column_id);
                        
                        if (column_id == 111)
                        {
                            coltype = Types.LONGNVARCHAR;
                        }
                        
                        if (column_id == 137)
                        {
                            coltype = Types.LONGNVARCHAR;
                        }

                        if (column_id == 124)
                        {
                        	coltype = Types.CHAR;
                        }
                        
                        if(column_id == 42 || column_id == 113 || column_id == 132)
                        {
                        	coltype = Types.BIT;
                        }


                        switch (coltype)
                        {
                        case Types.VARCHAR:
                        case Types.CHAR:
                        case Types.NVARCHAR:
                        case Types.LONGNVARCHAR:
                            p = PARAMETERS_MAP.get(String.class);
                            break;
                            
                        case Types.BINARY:
                            p = PARAMETERS_MAP.get(byte[].class);
                            String str = piece.get(i).get(md.getTableName(column_id)+"_"+md.getColumnName(column_id)).toString();
                            pstmt.setBytes(column_id, Bytes.toBytes(str));
                            break;
                            
                        case Types.TIMESTAMP:
                            p = PARAMETERS_MAP.get(Timestamp.class);
                            break;
                            
                        case Types.INTEGER:
                            p = PARAMETERS_MAP.get(Integer.class);
                            break;
                        case Types.BIT:
                            p = PARAMETERS_MAP.get(Boolean.class);
                            break;
                        case Types.TINYINT:
                            p = PARAMETERS_MAP.get(Integer.class);
                            break;
                        case Types.BIGINT:
                            p = PARAMETERS_MAP.get(Long.class);
                            break;
                        default:
                            LOGGER.error("Unexpected SQL Type");
                            continue;
                        }
                            
                        try
                        {                            
                            
                            if (piece.get(i).get(md.getTableName(column_id)+"_"+md.getColumnName(column_id)) == null)
                            {
                                pstmt.setNull(column_id, -1);
                            }
    
                            else
                            {
                            	 if(column_id == 42 || column_id == 113 || column_id == 132)
                                 {
                            		 p.set(pstmt, column_id, !piece.get(i).get(md.getTableName(column_id)+"_"+md.getColumnName(column_id)).toString().isEmpty());
                                 }
                            	 else
                            	 {
                            		 p.set(pstmt, column_id, piece.get(i).get(md.getTableName(column_id)+"_"+md.getColumnName(column_id)));
                            	 }
                            	
                                
                            }
                        }
                        catch (Exception e)
                        {
                            pstmt.setNull(column_id, -1);
                        }
                        
                    }
                }
                catch(Exception e)
                {
                    LOGGER.error("Unable to bind parameters");
                    e.printStackTrace();
                }
                
                try 
                {
                    pstmt.addBatch();
                } 
                catch (SQLException e) 
                {
                    LOGGER.error("Unable to add Prepared Statement to the batch");
                    e.printStackTrace();
                }
                i++;
            }
            
            try 
            {
                pstmt.executeBatch();
            } 
            
            catch (SQLException e) 
            {
                LOGGER.error("Unable to execute Batch");
                e.printStackTrace();
            }
            
        }
    }

    public static interface Parameter 
    {
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException;
    }

    public static class StringParameter implements Parameter 
    {
        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setString(index, (String) value);
        }

    }

    public static class LongParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setLong(index, (Long) value);
        }

    }

    public static class IntegerParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setInt(index, (Integer) value);
        }

    }

    public static class ByteParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setByte(index, (Byte) value);
        }

    }

    public static class ByteArrayParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setBytes(index, (byte[]) value);
        }

    }

    public static class ShortParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setShort(index, (Short) value);
        }

    }

    public static class DoubleParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setDouble(index, (Double) value);
        }
    }

    public static class TimestampParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setTimestamp(index, (Timestamp)value, getUTCCalendar());
        }

    }
    
    public static Calendar getUTCCalendar() 
    {
        return Calendar.getInstance(TimeZone.getTimeZone("UTC"));
    }

    public static class DateParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setDate(index, new java.sql.Date(((Date)value).getTime()));
        }
    }

    public static class UUIDParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setString(index, ((UUID)value).toString());
        }
    }

    public static class BooleanParameter implements Parameter 
    {

        @Override
        public void set(PreparedStatement p, int index, Object value)
                throws SQLException 
        {
            p.setBoolean(index, ((Boolean) value));
        }
    }
    

}


