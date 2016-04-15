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

public class ExportHelper1 {
    
    private static final String DB_DRIVER = "transbase.jdbc.Driver";
    private static final String DB_URL = "jdbc:transbase://127.0.0.1/";
    private static final String DB_DATABASE = "TECDOC_CD_1_2015";
    private static final String DB_USER = "tecdoc";
    private static final String DB_PASSWORD = "tcd_error_0";
    private Connection connection = null;
    private Connection mysqlConnection = null;
    private static final int UKRAINE_CODE = 210;
    private static final int RUSSIAN_ID = 16;
    
    public ExportHelper1(Connection conn) {
        try {
            mysqlConnection = conn;
            Class.forName(DB_DRIVER);
            connection = DriverManager.getConnection(DB_URL + DB_DATABASE, DB_USER, DB_PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
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
                if (tableName.indexOf("TOF_") != -1) {
                    System.out.println(tableName);
                }
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
    
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
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
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

            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery(sqlSetectOthers)) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                    //exportTableData(result, numberOfColumns, mysqlTable);
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
                        exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                        //exportTableData(result, numberOfColumns, mysqlTable);
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
    /**
     Duplicate entry '1070' for key 'PRIMARY'
     * table do nat exists
     */    
    public void exportModelsOther() {

        final String tableName = "TOF_MODELS";
        final String tableCountry = "TOF_COUNTRY_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_models_other";

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

            System.out.println("Export other models");

            st = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,ResultSet.CONCUR_READ_ONLY);
            try (ResultSet result = st.executeQuery("SELECT MOD_ID, MOD_MFA_ID, MOD_CDS_ID, MOD_PCON_START, MOD_PCON_END, MOD_PC, MOD_CV, MOD_AXL, TEX_TEXT \n" +
                    "FROM TOF_MODELS, TOF_COUNTRY_DESIGNATIONS, TOF_DES_TEXTS\n" +
                    "WHERE MOD_ID NOT IN (\n" +
                    "SELECT MOD_ID\n" +
                    "FROM TOF_MODELS, TOF_COUNTRY_DESIGNATIONS, TOF_DES_TEXTS\n" +
                    "WHERE (MOD_PC_CTM SUBRANGE (210 CAST INTEGER) = 1 \n" +
                    "    OR MOD_CV_CTM SUBRANGE (210 CAST INTEGER) = 1) \n" +
                    "    AND CDS_LNG_ID = 16 \n" +
                    "    AND CDS_TEX_ID = TEX_ID \n" +
                    "    AND MOD_CDS_ID = CDS_ID \n" +
                    "    AND CDS_CTM SUBRANGE (210 CAST INTEGER) = 1\n" +
                    ")")) {
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                
                mysqlConnection.createStatement().executeUpdate(sqlDropTable);
                mysqlConnection.createStatement().executeUpdate(sqlCreateTable);
                
                for (String sql : sqlIndexes) {
                    mysqlConnection.createStatement().executeUpdate(sql);
                }
                mysqlSt = mysqlConnection.createStatement();
                try (ResultSet rs = mysqlSt.executeQuery("SELECT * FROM "+mysqlTable)) {
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                    //exportTableData(result, numberOfColumns, mysqlTable);
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
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                    //exportTableData(result, numberOfColumns, mysqlTable);
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
                        exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                        //exportTableData(result, numberOfColumns, mysqlTable);
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
                        exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                        //exportTableData(result, numberOfColumns, mysqlTable);
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
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                    //exportTableData(result, numberOfColumns, mysqlTable);
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
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                }
                //exportTableData(result, numberOfColumns, mysqlTable);
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
                    exportTableDataPrepSt(result, rs, numberOfColumns, mysqlTable);
                }
                //exportTableData(result, numberOfColumns, mysqlTable);
            }
            st.close();
            mysqlSt.close();
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }        
    }
    /*exists!*/
        public void exportPictures() {
        
        final String mysqlTable = "tof_graphics";
        
        final String sqlDropTable = "DROP TABLE IF EXISTS " + mysqlTable;
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (" +
                "article_id int(11), " +
                "image VARCHAR(100)" +
                ") ENGINE=MYISAM DEFAULT CHARSET=utf8";
        
        
        final String[] sqlIndexes = {
                "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id)"
        };
        
        Statement st;
        Statement mysqlSt;
        long time = System.currentTimeMillis();
        try {
            
            System.out.println("Export pictures");
            
            st = connection.createStatement();
            try (ResultSet result = st.executeQuery("SELECT LGA_ART_ID, GRA_ID, "
                    + "GRA_TAB_NR, GRA_GRD_ID, DOC_EXTENSION, GRA_LNG_ID "
                    + "FROM TOF_LINK_GRA_ART, TOF_GRAPHICS, TOF_DOC_TYPES "
                    + "WHERE DOC_TYPE=GRA_DOC_TYPE "
                    + "AND LGA_GRA_ID=GRA_ID "
                    + "AND GRA_TAB_NR IS NOT NULL "
                    + "ORDER BY GRA_TAB_NR")) {
                /*
                mysqlSt = mysqlConnection.createStatement();
                mysqlSt.executeUpdate(sqlDropTable);
                mysqlSt = mysqlConnection.createStatement();
                mysqlSt.executeUpdate(sqlCreateTable);
                for (String sql : sqlIndexes) {
                    mysqlSt = mysqlConnection.createStatement();
                    mysqlSt.executeUpdate(sql);
                }
                */
                int count = 0;
                ArrayList<Short> tableNumber = new ArrayList<>();
                ArrayList<Integer> ids = new ArrayList<>();
                //ArrayList<Integer> articles = new ArrayList<>();
                while (result.next()) {
                    tableNumber.add(count, result.getShort(3));
                    ids.add(count, result.getInt(4));
                //    articles.add(count, result.getInt(1));
                    count++;
                    System.out.println(count);
                }
                /*
                String sql = "INSERT INTO " + mysqlTable + " VALUES (?, ?)";
                try (PreparedStatement ps = mysqlConnection.prepareStatement(sql)) {
                    mysqlConnection.setAutoCommit(false);
                    for (int i = 0; i < ids.size(); i++) {
                        
                        ps.setLong(1, articles.get(i));
                        System.out.println(tableNumber.get(i)+ "/" + ids.get(i) + ".jpg");
                        ps.setString(2, tableNumber.get(i)+ "/" + ids.get(i) + ".jpg");
                        ps.addBatch();
                        
                        if (i % 5000 == 4500) {
                            System.out.println(i);
                            ps.executeBatch();
                            mysqlConnection.commit();
                        }
                    }
                    ps.executeBatch();
                    mysqlConnection.commit();
                }
                */
                for (int i = 0; i < ids.size(); i++) {
                    if(tableNumber.get(i)<45){continue;}
                    File dir = new File("C:/images_tecdoc/" + tableNumber.get(i)+ "/");
                    if (!dir.exists()) {
                        dir.mkdir();
                    }
                    File file = new File("C:/images_tecdoc/" + tableNumber.get(i)+ "/" + ids.get(i) + ".jpg");
                    System.out.print(i + " " +tableNumber.get(i)+ "-" + ids.get(i));
                    if (file.exists()) {
                        System.out.println(" - skip");
                        continue;
                    }
                    System.out.println(" - write");
                    try (Statement st3 = connection.createStatement()) {
                        String sql = "SELECT GRD_GRAPHIC FROM TOF_GRA_DATA_"
                                + tableNumber.get(i)
                                + " WHERE GRD_ID=" + ids.get(i);
                        try (ResultSet result2 = st3.executeQuery(sql)) {
                            result2.next();
                            byte[] rawData = result2.getBytes(1);
                            //ByteArrayInputStream data = (ByteArrayInputStream) result2.getBinaryStream(1);
                            ByteArrayInputStream data = new ByteArrayInputStream(rawData);
                            String path = "C:/images_tecdoc/" + tableNumber.get(i)+ "/" + ids.get(i) + ".jpg";
                            System.out.println((smartImgConverter(path, data))?"Done":"Fail");
                        }
                    }
                }
            }
            st.close();
            //mysqlSt.close();
            mysqlConnection.setAutoCommit(true);
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        } catch (SQLException e) {
            e.printStackTrace();
        }
        
    }
        
    private boolean smartImgConverter(String path, ByteArrayInputStream data){
        ImageWriter writer = null;
        try{
            
            Iterator<ImageReader> iter = ImageIO.getImageReadersByFormatName("JPEG2000");
            ImageReader reader = iter.next();
            if(reader == null) {
                System.out.println("Could not locate any Readers for the JPEG 2000 format image.");
                return false;
            }
            BufferedImage image = ImageIO.read(data);
            File outputFile = new File(path);
            writer = ImageIO.getImageWritersByFormatName("jpeg").next();
            ImageWriteParam param = writer.getDefaultWriteParam();
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
            param.setCompressionQuality(0.9F);
            writer.setOutput(ImageIO.createImageOutputStream(outputFile));
            writer.write(null, new IIOImage(image, null, null),param);
            return true;
        }catch(IOException ex){
            ex.printStackTrace();
        }finally{
            if(writer == null){
                return false;
            }else{
                writer.dispose();
                return true;
            }
        }
    }
        
    private boolean convertImage(String path) {
        try {
            
            Iterator<ImageReader> iter = ImageIO.getImageReadersByFormatName("JPEG2000");
            ImageReader reader = iter.next();
 
            if(reader == null) {
                System.out.println("Could not locate any Readers for the JPEG 2000 format image.");
                return true;
            } else {
            //    System.out.println(reader.getFormatName());
            }
            
            BufferedImage image = null;
            image = ImageIO.read(new File(path));
            //http://www.mkyong.com/java/how-to-convert-byte-to-bufferedimage-in-java/
            File outputFile = new File(path);
            ImageWriter writer = ImageIO.getImageWritersByFormatName("jpeg").next();
            ImageWriteParam param = writer.getDefaultWriteParam();
            param.setCompressionMode(ImageWriteParam.MODE_EXPLICIT); // Needed see javadoc
            param.setCompressionQuality(0.9F); // Highest quality
            writer.setOutput(ImageIO.createImageOutputStream(outputFile));
            writer.write(null, new IIOImage(image, null, null), param);
            writer.dispose();
 
        } catch (IOException e) {
            e.printStackTrace();
        }
        return true;
    }
    
/*##############################################################    
  ##############################################################
  ##############################################################*/

    private void exportTableDataPrepSt(ResultSet rsTransbase, 
            ResultSet rsMySql,
            int colNumb, String table)throws SQLException{
        rsTransbase.setFetchSize(5000);//?
        boolean isRowsInSet = rsTransbase.last();
        if(!isRowsInSet) throw new SQLException("Empty set");
        int rowCount = rsTransbase.getRow();
        rsTransbase.beforeFirst();
        System.out.println(rowCount+" to be processed");
        ResultSetMetaData rsmdMySql = rsMySql.getMetaData();
        StringBuilder sb = new StringBuilder("INSERT INTO "+table+" VALUES (");
        for (int i = 0;i < colNumb;i++){
            sb.append("?,");
        }
        sb.deleteCharAt(sb.length()-1);
        sb.append(")");
        String sql = sb.toString();
        mysqlConnection.setAutoCommit(false);
        try(PreparedStatement ps = mysqlConnection.prepareStatement(sql)){
            int counter = 0;
            while(rsTransbase.next()){
                for(int i = 1; i <= colNumb; i++){
                    switch(rsmdMySql.getColumnTypeName(i)){
                        case "INT":
                            if(rsTransbase.getObject(i)==null){
                                ps.setNull(i, java.sql.Types.INTEGER);
                            }else{
                                ps.setInt(i, rsTransbase.getInt(i));
                            }
                            break;
                        case "TINYINT":
                            if(rsTransbase.getObject(i)==null){
                                ps.setNull(i, java.sql.Types.TINYINT);
                            }else{
                                ps.setByte(i, rsTransbase.getByte(i));
                            }
                            break;
                        case "SMALLINT":
                            if(rsTransbase.getObject(i)==null){
                                ps.setNull(i, java.sql.Types.SMALLINT);
                            }else{
                                ps.setShort(i, rsTransbase.getShort(i));
                            }
                            break;
                        case "VARCHAR":
                            if(rsTransbase.getObject(i)==null){
                                ps.setString(i, "NULL");
                            }else{
                                ps.setString(i, rsTransbase.getString(i));
                            }
                            break;
                        case "FLOAT":
                            if(rsTransbase.getObject(i)==null){
                                ps.setNull(i, java.sql.Types.FLOAT);
                            }else{
                                ps.setFloat(i, rsTransbase.getFloat(i));
                            }
                            break;
                        default:
                            throw new SQLException("Unknown type in switch");
                    }
                    //ps.addBatch();
                }
                ps.executeUpdate();
                if(counter != 0 && (counter % 5000) == 0){
                    System.out.println(counter+" : "+(counter*100/rowCount)+"%");
                }
                counter++;
            }
            System.out.println(counter+" : "+(counter*100/rowCount)+"%");
            ps.close();
        }catch(SQLException ex){
            mysqlConnection.rollback();
            ex.printStackTrace();
        }finally{
            mysqlConnection.commit();
            mysqlConnection.setAutoCommit(true);
            System.out.println("Table "+table+" completed");
        }
    }
    
    
    private void exportTableData(ResultSet result, int numberOfColumns, 
            String table) throws SQLException {
        int count = 0;
        int counter = 0;
        result.setFetchSize(2000);//575 rows in 2015
        while (true) {
            String sql = "INSERT INTO " + table + " VALUES";
            while (count != 2000 && result.next()) {
                //System.out.println(sql+"\n-------------------------");
                if (count != 0) {
                    sql += ',';
                }
                sql += "(";
                for (int i = 1; i <= numberOfColumns; i++) {
                    //System.out.println(sql);
                    if (result.getObject(i) == null) {
                        sql += "NULL";
                    } else {
                        sql += "'" + cleanString(result.getString(i)) + "'";
                    }
                    if (i != numberOfColumns) {
                        sql += ", ";
                    } else {
                        sql += " ";
                    }
                }
                sql += ")";
                count++;
                counter++;
            }
            if (count > 0) {
                try (Statement st = mysqlConnection.createStatement()) {
                    st.executeUpdate(sql);
                }
                System.out.println(counter);
                count = 0;    
            } else {
                break;
            }
        }
    }
    
    private String cleanString(String str) {
        str = str.replace("\\", "\\\\'");
        return str.replace("'", "\\'");
    }
    
    private String countTime(long time){
        Date date = new Date(System.currentTimeMillis()-time);
        SimpleDateFormat formatter = new SimpleDateFormat("HH:mm:ss:SSS");
        formatter.setTimeZone(TimeZone.getTimeZone("UTC"));
        return formatter.format(date);
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
        
        
}
