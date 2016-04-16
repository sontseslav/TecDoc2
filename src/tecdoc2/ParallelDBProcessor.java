/*
 * 
 */
package tecdoc2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

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
                                    list.add(null);
                                } else {
                                    list.add(rsTransbase.getInt(i));
                                }
                                break;
                            case "TINYINT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(null);
                                } else {
                                    list.add((byte)rsTransbase.getByte(i));
                                }
                                break;
                            case "SMALLINT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(null);
                                } else {
                                    list.add((short)rsTransbase.getShort(i));
                                }
                                break;
                            case "VARCHAR":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(null);
                                } else {
                                    list.add(rsTransbase.getString(i));
                                }
                                break;
                            case "FLOAT":
                                if (rsTransbase.getObject(i) == null) {
                                    list.add(null);
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
                    Thread.sleep(1000);
                }
            }catch(SQLException | InterruptedException e){e.printStackTrace();}
            isNext = false;
            System.out.println(Thread.currentThread().getName()+" terminated");
        }
    }
    
    private class TaskProcessQueue implements Runnable{
    
        @Override
        public void run(){
        }
    }
    
    private final Connection mysqlConnection;
    private final ResultSet rsTransbase, rsMySql;
    private final int colNumb;
    private final String table;
    private final BlockingQueue<Container> queue = new ArrayBlockingQueue<>(10000);
    private String sqlInsert;
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
       try{mysqlConnection.setAutoCommit(false);
           sqlInsert = prepareInsertQerry();
           ResultSetMetaData rsmdMySql = rsMySql.getMetaData();//move to thread
       }catch(SQLException e){e.printStackTrace();
       }finally{
           mysqlConnection.setAutoCommit(true);
       }
    }
    
    private String prepareInsertQerry() throws SQLException{
        rsTransbase.setFetchSize(5000);//?
        boolean isRowsInSet = rsTransbase.last();
        if(!isRowsInSet) throw new SQLException("Empty set");
        int rowCount = rsTransbase.getRow();
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
