/*
 * 
 */
package tecdoc2;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;

/**
 * may be Runnable
 * @author user
 */
public class ParallelDBProcessor {
    //http://stackoverflow.com/questions/23957581/java-array-with-multiple-data-types
    
    
    private final Connection mysqlConnection;
    private final ResultSet rsTransbase, rsMySql;
    private final int colNumb;
    private final String table;
    private String sqlInsert;
    
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
        ResultSetMetaData rsmdMySql = rsMySql.getMetaData();//move to thread
        StringBuilder sb = new StringBuilder("INSERT INTO "+table+" VALUES (");
        for (int i = 0;i < colNumb;i++){
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }
}
