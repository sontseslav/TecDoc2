package tecdoc2;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * may be Runnable
 * @author user
 */
public class ParallelDBProcessor {
    private final class Container{
        private final List<Object> container;
        
        public Container(List<Object> container){
            this.container = new ArrayList<>(container);
        }
        
        public List<Object> getContainer(){
            return new ArrayList<>(this.container);
        }
    }
    
    //http://stackoverflow.com/questions/23957581/java-array-with-multiple-data-types
    private class TaskFillQueue implements Runnable{
        private final ResultSet rsTransbase;
        private final int colNumber;
        private final ResultSetMetaData rsmdMySql;
        private final BlockingQueue<Container> queue;
        
        public TaskFillQueue(ResultSet rsTransbase, ResultSetMetaData rsmdMySql,
                int colNumber, BlockingQueue<Container> queue){
            this.rsTransbase = rsTransbase;
            this.rsmdMySql = rsmdMySql;
            this.queue = queue;
            this.colNumber = colNumber;
        }
        /**
         * Only one thread
         */
        @Override
        public void run(){
            int counter = 0;
            try{
                while(rsTransbase.next()){
                    List<Object> list = new ArrayList<>();
                    for(int i = 1; i <= colNumber; i++){
                        switch (rsmdMySql.getColumnTypeName(i)) {
                            case "INT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(-99);
                                } else {
                                    list.add((int)rsTransbase.getInt(i));
                                }
                                break;
                            case "TINYINT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(-99);
                                } else {
                                    list.add((byte)rsTransbase.getByte(i));
                                }
                                break;
                            case "SMALLINT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(-99);
                                } else {
                                    list.add((short)rsTransbase.getShort(i));
                                }
                                break;
                            case "VARCHAR":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add("-99");
                                } else {
                                    list.add(rsTransbase.getString(i));
                                }
                                break;
                            case "FLOAT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(-99);
                                } else {
                                    list.add((float)rsTransbase.getFloat(i));
                                }
                                break;
                            default:
                                throw new SQLException("Unknown type in switch");
                        }
                    }
                    queue.put(new Container(list));
                    counter++;
                    if(counter % 10000 == 0){
                        System.out.println( counter + " objects contained");
                    }
                }
                while(!queue.isEmpty()){
                    Thread.sleep(100);
                }
            }catch(SQLException | InterruptedException e){e.printStackTrace();}
            isNext = false;
            System.out.println(Thread.currentThread().getName()+" terminated");
        }
    }
    
    private class TaskProcessQueue implements Runnable{
        private final BlockingQueue<Container> queue;
        private final String sqlInsert;
        private final Connection connMySQL;
        
        public TaskProcessQueue(BlockingQueue<Container> queue,String sqlInsert,
                Connection connMySQL){
            this.queue = queue;
            this.sqlInsert = sqlInsert;
            this.connMySQL = connMySQL;
        }
        
        @Override
        public void run(){
            try(PreparedStatement ps = connMySQL.prepareStatement(sqlInsert)){
                int batchElements = 1;
                while(!queue.isEmpty()){//even if I enpty NOW I'll be full in the future.
                    List<Object> list = queue.take().getContainer();
                    int i = 1; //column count
                    for (Object element : list){//process row
                        switch(element.getClass().getSimpleName()){
                            case "Integer":
                                if ((int)element == -99) {
                                    ps.setNull(i, java.sql.Types.INTEGER);
                                } else {
                                    ps.setInt(i, (int)element);
                                }
                                break;
                            case "Byte":
                                if ((byte)element == -99) {
                                    ps.setNull(i, java.sql.Types.TINYINT);
                                } else {
                                    ps.setByte(i, (byte)element);
                                }
                                break;
                            case "Short":
                                if ((short)element == -99) {
                                    ps.setNull(i, java.sql.Types.SMALLINT);
                                } else {
                                    ps.setShort(i, (short)element);
                                }
                                break;
                            case "String":
                                if (element.toString().equals("-99")) {
                                    ps.setString(i, null);
                                } else {
                                    ps.setString(i, element.toString());
                                }
                                break;
                            case "Float":
                                if ((float)element == -99) {
                                    ps.setNull(i, java.sql.Types.FLOAT);
                                } else {
                                    ps.setFloat(i, (float)element);
                                }
                                break;
                            default:
                                throw new SQLException("Unknown type in switch");
                        }
                        i++;
                    }
                    ps.addBatch();
                    if(batchElements % 1000 == 0 || queue.isEmpty()){
                        ps.executeBatch();
                    }
                    batchElements++;
                    int counter;
                    boolean updated;
                    do {
                        counter = rowsSet.get();
                        updated = rowsSet.compareAndSet(counter, ++counter);
                    } while (!updated);
                    if (counter % 10000 == 0) {
                        System.out.println(Thread.currentThread().getName()+" : "
                                +counter + " : " + (counter/rowCount*100) + "%");
                    }
                    if (counter % 100000 == 0){mysqlConnection.commit();}
                }
            }catch(SQLException | InterruptedException e){
                e.printStackTrace();
                System.out.println(Thread.currentThread().getName()+" giving up...");
            }
        }
    }
    
    private final Connection mysqlConnection;
    private final ResultSet rsTransbase, rsMySql;
    private final int colNumb;
    private final String table;
    private final BlockingQueue<Container> queue = new ArrayBlockingQueue<>(200000);
    private final AtomicInteger rowsSet = new AtomicInteger(0);
    private final int corePoolSize = 5;
    private final int maxPoolSize = 10;
    private final int keepAliveTime = 5000;
    private String sqlInsert;
    private int rowCount;
    private boolean isNext = true;
    
    public ParallelDBProcessor(Connection mysqlConnection, ResultSet rsTransbase, 
            ResultSet rsMySql, int colNumb, String table){
        this.mysqlConnection = mysqlConnection;
        this.rsTransbase = rsTransbase;
        this.rsMySql = rsMySql;
        this.colNumb = colNumb;
        this.table = table;
    }
    
    public void exec()throws SQLException{//if only finally block fails
       try{
           mysqlConnection.setAutoCommit(false);//care about data consistency costs performance
           sqlInsert = prepareInsertQuery();
           ResultSetMetaData rsmdMySql = rsMySql.getMetaData();
           List<Future> futureProcList = new ArrayList<>();
           Thread filler = new Thread(new TaskFillQueue(rsTransbase, rsmdMySql, 
                   colNumb, queue));
           filler.setName("Filler");
           filler.start();
           ExecutorService threadPoolExecutor = new ThreadPoolExecutor(corePoolSize,
           maxPoolSize, keepAliveTime, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
           for(int i = 0; i < maxPoolSize; i++){
               futureProcList.add(threadPoolExecutor.submit(new TaskProcessQueue(queue, sqlInsert, 
                       mysqlConnection)));
           }
           while(isNext){//is queue is empty and Filler done it job?
               try{
                   Thread.sleep(500);
                   //System.out.println("waiting...");
               }catch(InterruptedException e){e.printStackTrace();}
           }
           //try{Thread.sleep(500);}catch(InterruptedException e){}//waiting for a shure...
           for(Future future : futureProcList){
               future.cancel(true);
           }
           threadPoolExecutor.shutdown(); //exit is safe even if some threads was waiting in a queue, they definitely gave up
           System.out.println("Table "+table+" completed");
       }catch(SQLException e){
           mysqlConnection.rollback();
           e.printStackTrace();
       }finally{
           mysqlConnection.commit();
           mysqlConnection.setAutoCommit(true);
       }
    }
    
    private String prepareInsertQuery() throws SQLException{
        rsTransbase.setFetchSize(5000);//?
        boolean isRowsInSet = rsTransbase.last();
        if(!isRowsInSet) throw new SQLException("Empty set");
        rowCount = rsTransbase.getRow();
        rsTransbase.beforeFirst();
        System.out.println(rowCount+" to be processed");
        
        StringBuilder sb = new StringBuilder("INSERT INTO "+table+" VALUES (");
        for (int i = 0;i < colNumb;i++){
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }
}