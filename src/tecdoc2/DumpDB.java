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
    private static final String MYSQL_DB_NAME = "tecdoc2015q1";
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
    
    public void dumpManufacturersUA() {
        final String tableName = "TOF_MANUFACTURERS";
        final String mysqlTable = "tof_manufacturers_ua";

        final String sqlCreateTable = "CREATE TABLE `" + mysqlTable + "` (\n" +
                                    "  `id` int(11) NOT NULL,\n" +
                                    "  `passenger_car` tinyint(4) DEFAULT NULL,\n" +
                                    "  `commercial_vehicle` tinyint(4) DEFAULT NULL,\n" +
                                    "  `axle` tinyint(4) DEFAULT NULL,\n" +
                                    "  `engine` tinyint(4) DEFAULT NULL,\n" +
                                    "  `engine_type` tinyint(4) DEFAULT NULL,\n" +
                                    "  `code` varchar(20) DEFAULT NULL,\n" +
                                    "  `brand` varchar(100) DEFAULT NULL,\n" +
                                    "  `number` smallint(6) DEFAULT NULL,\n" +
                                    "  PRIMARY KEY (`id`)\n" +
                                    ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        final String sqlSetectUA = "SELECT MFA_ID, MFA_PC_MFC, MFA_CV_MFC," +
                    " MFA_AXL_MFC, MFA_ENG_MFC, MFA_ENG_TYP, MFA_MFC_CODE, MFA_BRAND," +
                    " MFA_MF_NR FROM " + tableName + " WHERE " +
                    " MFA_CV_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR" +
                    " MFA_PC_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1";

        long time = System.currentTimeMillis();
        System.out.println("Start dumping manufacturers for UA");
        try(Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet result = st.executeQuery(sqlSetectUA)){
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, "\n");
            }catch (SQLException e) {e.printStackTrace();}
        System.out.println("Time elapsed: "+countTime(time)
                +"\n----------------------------");
    }
    
    
    public void dumpArticlesLookupUA(){
        final String tableName = "TOF_ART_LOOKUP";
        final String mysqlTable = "tof_articles_lookup_new_ua";
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "article_id int(11) NOT NULL,\n" +
                "search varchar(105) DEFAULT NULL,\n" +
                "display varchar(105) DEFAULT NULL,\n" +
                "article_type smallint(11) DEFAULT NULL,\n" +
                "brand_id int(11) DEFAULT NULL\n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (search);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_type);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (brand_id);\n";
        
        System.out.println("Start dumping UA articles lookup table");
        long time = System.currentTimeMillis();
            try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
                    ResultSet result = st.executeQuery("SELECT ARL_ART_ID, " +
                    "ARL_SEARCH_NUMBER, ARL_DISPLAY_NR, ARL_KIND, ARL_BRA_ID" +
                    " FROM " + tableName + 
                    " WHERE ARL_CTM SUBRANGE ("+UKRAINE_CODE+" CAST INTEGER) = 1" +
                    " ORDER BY ARL_ART_ID")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
            } catch (SQLException e){e.printStackTrace();}
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        
    }
    
    public void closeTransbase(){
        try{
            connTransbase.close();
        }catch(SQLException e){e.printStackTrace();}
    }
    
    /**
     * core method
     * creates dump of given table
     * @param ResultSet rsTransbase,int columnCount,String mysqlTable
     * @return None
     * @throws SQLException
     */
    private void makeDump(ResultSet rsTransbase,int columnCount,
            String mysqlTable,String sqlCreateTable,String sqlIndexes)throws SQLException{
        rsTransbase.setFetchSize(5000);//?
        boolean isRowsInSet = rsTransbase.last();
        if(!isRowsInSet) throw new SQLException("Empty set");
        int rowCount = rsTransbase.getRow();
        rsTransbase.beforeFirst();
        System.out.println(rowCount+" to be processed");
        
        DumpFile df = DumpFile.dumpFileFactory(mysqlTable+".sql");
        //write intro
        String textToWrite = "-- MySQL dump 10.16  Distrib 10.1.13-MariaDB, for Linux (i686)\n" +
                    "--\n" +
                    "-- Host: localhost    Database: "+MYSQL_DB_NAME+"\n" +
                    "-- ------------------------------------------------------\n" +
                    "-- Server version	10.1.13-MariaDB\n" +
                    "\n" +
                    "/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;\n" +
                    "/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;\n" +
                    "/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;\n" +
                    "/*!40101 SET NAMES utf8 */;\n" +
                    "/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;\n" +
                    "/*!40103 SET TIME_ZONE='+00:00' */;\n" +
                    "/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;\n" +
                    "/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;\n" +
                    "/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;\n" +
                    "/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;\n" +
                    "\n" +
                    "--\n" +
                    "-- Table structure for table `"+mysqlTable+"`\n" +
                    "--\n" +
                    "\n" +
                    "DROP TABLE IF EXISTS `"+mysqlTable+"`;\n" +
                    "/*!40101 SET @saved_cs_client     = @@character_set_client */;\n" +
                    "/*!40101 SET character_set_client = utf8 */;\n";
        df.makeWrite(textToWrite);
        //write create table and indexes
        df.makeWrite(sqlCreateTable + "/*!40101 SET character_set_client = @saved_cs_client */;\n");
        df.makeWrite(sqlIndexes);
        //write insert introduction
        textToWrite = "--\n" +
                    "-- Dumping data for table `"+mysqlTable+"`\n" +
                    "--\n" +
                    "\n" +
                    "LOCK TABLES `"+mysqlTable+"` WRITE;\n" +
                    "/*!40000 ALTER TABLE `"+mysqlTable+"` DISABLE KEYS */;\n";
        df.makeWrite(textToWrite);
        //db iteration
        long row = 0;
        String insert = "INSERT INTO `"+mysqlTable+"` VALUES ";
        StringBuilder sb = new StringBuilder(insert);
        ResultSetMetaData rsmdTransbase = rsTransbase.getMetaData();
            try{
                while(rsTransbase.next()){
                    for(int i = 1; i <= columnCount; i++){
                        if(i == 1) sb.append("(");
                        //System.out.printf("%d colump has type %s%n", i, rsmdTransbase.getColumnTypeName(i));
                        switch (rsmdTransbase.getColumnTypeName(i)) {
                            case "INT":
                            case "INTEGER":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    sb.append((int)rsTransbase.getInt(i));
                                }
                                if(i == columnCount){
                                    sb.append(")");
                                }else{
                                    sb.append(",");
                                }
                                break;
                            case "TINYINT":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    sb.append((byte)rsTransbase.getByte(i));
                                }
                                if(i == columnCount){
                                    sb.append(")");
                                }else{
                                    sb.append(",");
                                }
                                break;
                            case "SMALLINT":
                            case "CHAR":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    sb.append((short)rsTransbase.getShort(i));
                                }
                                if(i == columnCount){
                                    sb.append(")");
                                }else{
                                    sb.append(",");
                                }
                                break;
                            case "VARCHAR":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    String s = rsTransbase.getString(i).replace("'", "");
                                    if(s.endsWith("\\")){s = s.substring(0, s.length()-1);}
                                    sb.append("'").append(s).append("'");
                                }
                                if(i == columnCount){
                                    sb.append(")");
                                }else{
                                    sb.append(",");
                                }
                                break;
                            case "FLOAT":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    sb.append((float)rsTransbase.getFloat(i));
                                }
                                if(i == columnCount){
                                    sb.append(")");
                                }else{
                                    sb.append(",");
                                }
                                break;
                            default:
                                throw new SQLException("Unknown type in switch");
                        }
                    }
                    row++;
                    if(row % 15000 == 0){
                        sb.append(";\n");
                        df.makeWrite(sb.toString());
                        sb = new StringBuilder(insert);
                    }else{
                        sb.append(",");
                    }
                    if(row % 100000 == 0){System.out.printf("%d rows added : %.2f %n",row,(((double)row)/rowCount*100));}
                }
                System.out.printf("%d rows added : %.2f %n",row,(((double)row)/rowCount*100));
                if(sb.length() == insert.length()){
                    sb.replace(0, sb.length()-1, "\n");
                }else{
                    sb.deleteCharAt(sb.length()-1);
                    sb.append(";\n");
                }
                df.makeWrite(sb.toString());
            }catch(SQLException e){e.printStackTrace();}
        //write outro
        Date date = new Date(System.currentTimeMillis());
        SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        formatter.setTimeZone(TimeZone.getDefault());
        textToWrite = "/*!40000 ALTER TABLE `"+mysqlTable+"` ENABLE KEYS */;\n" +
                    "UNLOCK TABLES;\n" +
                    "/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;\n" +
                    "\n" +
                    "/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;\n" +
                    "/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;\n" +
                    "/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;\n" +
                    "/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;\n" +
                    "/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;\n" +
                    "/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;\n" +
                    "/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;\n" +
                    "\n" +
                    "-- Dump completed on "+formatter.format(date)+"\n";
        df.makeWrite(textToWrite);
        //flushing and closing
        df.close();
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
