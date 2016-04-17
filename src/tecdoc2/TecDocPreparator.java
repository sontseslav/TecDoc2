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
            ResultSet result = st.executeQuery("SELECT * FROM systable WHERE "
                    + "TNAME LIKE 'TOF_%'");
            while (result.next()) {
                String tableName = result.getString(1);
                // Just TecDoc
                System.out.println(tableName);
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

            transbaseSt = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = transbaseSt.executeQuery(sqlSetectUA)) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
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
    
    public void exportManufacturersOther() {

        final String tableName = "TOF_MANUFACTURERS";
        final String mysqlTable = "tof_manufacturers_other";

        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "id int(11) PRIMARY KEY, " +
                "passenger_car TINYINT, " +
                "commercial_vehicle TINYINT, " +
                "axle TINYINT, " +
                "engine TINYINT, " +
                "engine_type TINYINT, " +
                "code VARCHAR(20), " +
                "brand VARCHAR(100), " +
                "number SMALLINT" +
                ")";
        final String sqlSetectOthers = "SELECT MFA_ID, MFA_PC_MFC, MFA_CV_MFC," 
                + " MFA_AXL_MFC, MFA_ENG_MFC, MFA_ENG_TYP, MFA_MFC_CODE, "
                + "MFA_BRAND, MFA_MF_NR FROM " + tableName +
                " WHERE MFA_ID NOT IN "
                + "("
                + "SELECT MFA_ID FROM "+tableName+" WHERE "
                + "MFA_PC_CTM SUBRANGE (210 CAST INTEGER) = 1" 
                + " OR MFA_CV_CTM SUBRANGE (210 CAST INTEGER) = 1"
                + ")";


        Statement st;
        Statement mysqlSt;
        long time = System.currentTimeMillis();
        try {

            System.out.println("Export other manufacturers");

            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery(sqlSetectOthers)) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                }
            }
            st.close();
            mysqlSt.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
    public void exportModelsUA() {
            
            final String tableName = "TOF_MODELS";
            final String tableCountry = "TOF_COUNTRY_DESIGNATIONS";
            final String tableDescriptions = "TOF_DES_TEXTS";
            final String mysqlTable = "tof_models_ua";
            
            final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
            final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                    "id INT(11) PRIMARY KEY, " +
                    "manufacturer_id int(11), " +
                    "description_id int(11), " +
                    "start_date int(6), " +
                    "end_date int(6), " +
                    "passenger_car TINYINT, " +
                    "commercial_vehicle TINYINT, " +
                    "axle TINYINT, " +
                    "description VARCHAR(255)" +
                    ")";
     
            final String[] sqlIndexes = {
                    "ALTER TABLE " + mysqlTable + " ADD INDEX (manufacturer_id)"
            };
            
            
            Statement st;
            Statement mysqlSt;
            long time = System.currentTimeMillis();
            try {
                
                System.out.println("Export UA models");
                
                st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                try (ResultSet result = st.executeQuery("SELECT MOD_ID, MOD_MFA_ID, MOD_CDS_ID," +
                        " MOD_PCON_START, MOD_PCON_END, MOD_PC, MOD_CV, MOD_AXL, TEX_TEXT " +
                        " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE" +
                        " (MOD_PC_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR" +
                        " MOD_CV_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND" +
                        " CDS_LNG_ID = " + RUSSIAN_ID + " AND CDS_TEX_ID = TEX_ID AND MOD_CDS_ID = CDS_ID" +
                        " AND  CDS_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1")) {
                    ResultSetMetaData metaResult = result.getMetaData();
                    int numberOfColumns = metaResult.getColumnCount();
                    
                    mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                    mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                    
                    for (String sql : sqlIndexes) {
                        mysqlConnection.createStatement().executeUpdate(sql);
                    }
                    mysqlSt = mysqlConnection.createStatement();
                    try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                        new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                    }
                }
                st.close();
                mysqlSt.close();
                System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    
    public void exportTypesUA() {
        
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_COUNTRY_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_ua";
        
        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "id int(11) PRIMARY KEY," +
                "model_id int(11)," +
                "start_date int(6)," +
                "end_date int(6)," +
                "description varchar(100)," +
                "capacity float(5,1), " +
                "capacity_hp_from int(5)," +
                "capacity_kw_from int(5)" +
            ")";
        
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (model_id)"
        };
        
        Statement st;
        Statement mysqlSt;
        long time = System.currentTimeMillis();
        try {
            
            System.out.println("Export UA types");
            
            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery("SELECT TYP_ID, TYP_MOD_ID,  TYP_PCON_START, TYP_PCON_END, TEX_TEXT, TYP_LITRES, TYP_HP_FROM, TYP_KW_FROM" +
                    " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE" +
                    " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR" +
                    " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND" +
                    " CDS_LNG_ID = " + RUSSIAN_ID + " AND CDS_TEX_ID = TEX_ID AND TYP_CDS_ID = CDS_ID" +
                    " AND  CDS_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                }
            }
            st.close();
            mysqlSt.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

        public void exportArticlesUA() {
            
            final String tableName = "TOF_ARTICLES";
            final String tableCountry = "TOF_DESIGNATIONS";
            final String tableDescriptions = "TOF_DES_TEXTS";
            final String mysqlTable = "tof_articles_new_ua";
            
            final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
            final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                    "id int(11), " +
                    "article_nr VARCHAR(80), " +
                    "supplier_id int(11), " +
                    "description VARCHAR(1024), " +
                    "PRIMARY KEY (id)" +
                    ")";
            
            final String[] sqlIndexes = {
                    "ALTER TABLE " + mysqlTable + " ADD INDEX (supplier_id)",
                    "ALTER TABLE " + mysqlTable + " ADD INDEX (article_nr)"
            };
            
            System.out.println("Export UA articles");
                    
            Statement st;
            Statement mysqlSt;
            long time = System.currentTimeMillis();
            try {
                
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
            
                st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                try (ResultSet result = st.executeQuery("SELECT ART_ID, ART_ARTICLE_NR, ART_SUP_ID, TEX_TEXT" +
                        " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " " +
                        " WHERE ART_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 AND " +
                        " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND " +
                        " ART_COMPLETE_DES_ID = DES_ID")) {
                    ResultSetMetaData metaResult = result.getMetaData();
                    int numberOfColumns = metaResult.getColumnCount();
                    
                    mysqlSt = mysqlConnection.createStatement();
                    try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                        new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                    }
                }
                st.close();
                mysqlSt.close();
                System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }        
        
    public void exportSuppliersUA() {
            
            final String tableName = "TOF_SUPPLIERS";
            final String mysqlTable = "tof_suppliers_ua";
            
            final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
            final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                    "id int(11), " +
                    "brand VARCHAR(100), " +
                    "alias VARCHAR(100), " +
                    "supplier_nr int(11), " +
                    "PRIMARY KEY (id)" +
                    ")";
            
            final String[] sqlIndexes = {
                    "ALTER TABLE " + mysqlTable + " ADD INDEX (brand)",
                    "ALTER TABLE " + mysqlTable + " ADD INDEX (supplier_nr)"
            };
        
            System.out.println("Export UA suppliers");
            
            Statement st;
            Statement mysqlSt;
            long time = System.currentTimeMillis();
            try {
     
     
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                
                st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
                try (ResultSet result = st.executeQuery("SELECT DISTINCT SUP_ID, SUP_BRAND, SUP_BRAND, SUP_SUPPLIER_NR" +
                        " FROM " + tableName)) {
                    ResultSetMetaData metaResult = result.getMetaData();
                    int numberOfColumns = metaResult.getColumnCount();
                    mysqlSt = mysqlConnection.createStatement();
                    try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                        new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                    }
                }
                st.close();
                mysqlSt.close();
                System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        
    public void exportArticlesLookupUA() {
        final String tableName = "TOF_ART_LOOKUP";
        final String mysqlTable = "tof_articles_lookup_new_ua";
        
        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "article_id int(11), " +
                "search varchar(105), " +
                "display varchar(105), " +
                "article_type smallint(11), " +
                "brand_id int(11) " +
                ")";
        
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (search)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_type)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (brand_id)"
        };
        
        System.out.println("Export UA articles lookup table");
        
        Statement st;
        Statement mysqlSt;
        long time = System.currentTimeMillis();
        try {
            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery("SELECT ARL_ART_ID, ARL_SEARCH_NUMBER, ARL_DISPLAY_NR, ARL_KIND, ARL_BRA_ID" +
                    " FROM " + tableName + " ORDER BY ARL_ART_ID")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                }
            }
            mysqlSt.close();
            st.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }        
    }
    
    public void exportSearchTree() {
        final String tableName = "TOF_SEARCH_TREE";
        final String mysqlTable = "tof_search_tree_ua";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        
        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "id INT(11), " +
                "parent_id INT(11), " +
                "type SMALLINT(2), " +
                "level SMALLINT(2), " +
                "node_number INT(11), " +
                "sort INT(11), " +
                "text VARCHAR(255), " +
                "PRIMARY KEY (id)" +
                ")";
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (level)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (sort)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (type)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (parent_id)"
        };
        long time = System.currentTimeMillis();
        System.out.println("Export UA search tree table");
        Statement st;
        Statement mysqlSt;
        try {
            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery("SELECT DISTINCT STR_ID, "
                    + "STR_ID_PARENT, STR_TYPE, STR_LEVEL, STR_SORT, "
                    + "STR_NODE_NR, TEX_TEXT" 
                    + " FROM " + tableName + ", " + tableCountry + ", " 
                    + tableDescriptions + " "
                    + " WHERE DES_LNG_ID = " + RUSSIAN_ID 
                    + " AND DES_TEX_ID = TEX_ID AND " 
                    + " DES_ID=STR_DES_ID")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                mysqlSt = mysqlConnection.createStatement();
                try(ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)){
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                }
            }
            mysqlSt.close();
            st.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }        
    }
    
    public void exportLinkGenericArticleSearchTree() {
        final String tableName = "TOF_LINK_GA_STR";
        final String mysqlTable = "tof_link_generic_article_search_tree_ua";
        
        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "search_tree_id INT(11), " +
                "generic_article_id INT(11)" +
                ")";
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (search_tree_id)",
                "ALTER TABLE " + mysqlTable + " ADD INDEX (generic_article_id)"
        };
        long time = System.currentTimeMillis();
        System.out.println("Export UA link generic article search tree table");
        Statement st;
        Statement mysqlSt;
        try {
            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery("SELECT DISTINCT LGS_STR_ID, LGS_GA_ID" +
                    " FROM " + tableName)) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                mysqlSt = mysqlConnection.createStatement();
                try(ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)){
                    new ParallelDBProcessor(mysqlConnection, result, rs, 
                            numberOfColumns, mysqlTable).exec();
                }
            }
            st.close();
            mysqlSt.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }        
    }
    
    public void closeConnection(){
        try {
            connection.close();
            mysqlConnection.close();
            System.out.println("Closed");
        } catch (SQLException ex) {
            ex.printStackTrace();
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
