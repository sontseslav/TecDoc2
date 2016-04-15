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

public class TecDocPreparator {
    private static final String DB_DRIVER = "transbase.jdbc.Driver";
    private static final String DB_URL = "jdbc:transbase://127.0.0.1/";
    private static final String DB_DATABASE = "TECDOC_CD_1_2015";
    private static final String DB_USER = "tecdoc";
    private static final String DB_PASSWORD = "tcd_error_0";
    private static final int UKRAINE_CODE = 210;
    private static final int RUSSIAN_ID = 16;
    private Connection connection = null;
    private Connection mysqlConnection = null;
    
    public TecDocPreparator(Connection conn) {
        try {
            mysqlConnection = conn;
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_URL + DB_DATABASE, DB_USER, DB_PASSWORD);
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    public void printSysTable() {
        Statement st;
        try {
            st = connection.createStatement();
            ResultSet result = st.executeQuery("SELECT * FROM systable");
            while (result.next()) {
                String tableName = result.getString(1);
                // Just TecDoc
                if (tableName.contains("TOF_")) {
                    System.out.println(tableName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    /**
     * Exports ua manufacturers table
     */
    public void exportManufacturersUA() {
        final String tableName = "TOF_MANUFACTURERS";
        final String mysqlTable = "tof_manufacturers_ua";

        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = "CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "id INT(11)  PRIMARY KEY, " +
                "passenger_car TINYINT, " +
                "commercial_vehicle TINYINT, " +
                "axle TINYINT, " +
                "engine TINYINT, " +
                "engine_type TINYINT, " +
                "code VARCHAR(20), " +
                "brand VARCHAR(100), " +
                "number SMALLINT" +
                ")";
        final String sqlSetectUA = "SELECT MFA_ID, MFA_PC_MFC, MFA_CV_MFC," +
                    " MFA_AXL_MFC, MFA_ENG_MFC, MFA_ENG_TYP, MFA_MFC_CODE, MFA_BRAND," +
                    " MFA_MF_NR FROM " + tableName + " WHERE " +
                    " MFA_CV_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR" +
                    " MFA_PC_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1";


        Statement transbaseSt;
        Statement mysqlSt;
        long time = System.currentTimeMillis();
        try {

            System.out.println("Export manufacturers for UA");

            transbaseSt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = transbaseSt.executeQuery(sqlSetectUA)) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    //exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                    //exportTableData(result, numberOfColumns, mysqlTable);
                }
            }
            mysqlSt.close();
            transbaseSt.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
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
