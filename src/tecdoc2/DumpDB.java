package tecdoc2;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Iterator;
import java.util.TimeZone;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
/**
 *
 * @author coder007
 */
public class DumpDB {
    private static final String DB_DRIVER = "transbase.jdbc.Driver";
    private static final String DB_URL = "jdbc:transbase://127.0.0.1/";
    private static final String DB_DATABASE = "TECDOC_CD_1_2015";
    private static final String DB_USER = "tecdoc";
    private static final String DB_PASSWORD = "tcd_error_0";
    private static final int UKRAINE_CODE = 210;
    private static final int RUSSIAN_ID = 16;
    private Connection connTransbase = null;
    
    public DumpDB(){
        try {
            Class.forName(DB_DRIVER);
            connTransbase = DriverManager.getConnection(DB_URL + DB_DATABASE, DB_USER, DB_PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public void dumpArticlesLookupUA(){
        final String tableName = "TOF_ART_LOOKUP";
        final String mysqlTable = "tof_articles_lookup_new_ua";
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (search)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_type)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (brand_id)"
        };
        
        System.out.println("Start dumping UA articles lookup table");
        long time = System.currentTimeMillis();
            try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
                    ResultSet result = st.executeQuery("SELECT ARL_ART_ID, "
                    + "ARL_SEARCH_NUMBER, ARL_DISPLAY_NR, ARL_KIND, ARL_BRA_ID" +
                    " FROM " + tableName + " ORDER BY ARL_ART_ID")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                
                
            } catch (SQLException e){e.printStackTrace();}
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        
    }
    
    /**
     * core method
     * creates dump of given table
     * @param ResultSet rsTransbase,int columnCount,String mysqlTable
     * @return None
     * @throws SQLException
     */
    private void makeDump(ResultSet rsTransbase,int columnCount,
            String mysqlTable)throws SQLException{
        rsTransbase.setFetchSize(5000);//?
        boolean isRowsInSet = rsTransbase.last();
        if(!isRowsInSet) throw new SQLException("Empty set");
        int rowCount = rsTransbase.getRow();
        rsTransbase.beforeFirst();
        System.out.println(rowCount+" to be processed");
        /*
        1) prepare output sql file
        2) write standart start info
        3) iterate rs,create insert strings via StringBuilder (capacity 1500),
           write insert strings to file
        4) write standart end info
        5) flush buffer, close file
        */
    }
    /**
     * accessory method
     * @param Time in milliseconds
     * @return Data formated time
     */
    private String prepareInsertQuery(ResultSet rsTransbase,int columnCount,
            String mysqlTable) {
        
        
        StringBuilder sb = new StringBuilder("INSERT INTO "+mysqlTable+" VALUES (");
        for (int i = 0;i < columnCount;i++){
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");
        return sb.toString();
    }
    /**
     * accessory method
     * @param Time in milliseconds
     * @return Data formated time
     */
    private String countTime(long time){
        Date date = new Date(System.currentTimeMillis()-time);
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(date);
    }
}
