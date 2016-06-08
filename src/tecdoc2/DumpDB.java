package tecdoc2;
import java.awt.image.BufferedImage;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.sql.Blob;
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
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
/**
 *
 * @author coder007
 * 
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
        final String setCompressionVar = "SET GLOBAL innodb_file_per_table=1;\n"
                + "SET GLOBAL innodb_file_format=Barracuda;\n";
        final String initCompression = "ROW_FORMAT=COMPRESSED\n"
                + "KEY_BLOCK_SIZE=8\n";
        final String sqlCreateTable = setCompressionVar+
                " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "article_id int(11) NOT NULL,\n" +
                "search varchar(105) DEFAULT NULL,\n" +
                "display varchar(105) DEFAULT NULL,\n" +
                "article_type smallint(11) DEFAULT NULL,\n" +
                "brand_id int(11) DEFAULT NULL\n" +
                ") "+initCompression
                + " ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
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
    
    public void dumpManufacturersOther() {

        final String tableName = "TOF_MANUFACTURERS";
        final String mysqlTable = "tof_manufacturers_other";
        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "id int(11) PRIMARY KEY, \n" +
                "passenger_car TINYINT, \n" +
                "commercial_vehicle TINYINT, \n" +
                "axle TINYINT, \n" +
                "engine TINYINT, \n" +
                "engine_type TINYINT, \n" +
                "code VARCHAR(20), \n" +
                "brand VARCHAR(100), \n" +
                "number SMALLINT \n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        final String sqlIndexes = "\n";
        final String sqlSetectOthers = "SELECT MFA_ID, MFA_PC_MFC, MFA_CV_MFC," 
                + " MFA_AXL_MFC, MFA_ENG_MFC, MFA_ENG_TYP, MFA_MFC_CODE, "
                + "MFA_BRAND, MFA_MF_NR FROM " + tableName +
                " WHERE MFA_ID NOT IN "
                + "("
                + "SELECT MFA_ID FROM "+tableName+" WHERE "
                + "MFA_PC_CTM SUBRANGE (210 CAST INTEGER) = 1"  /*what the f*ck???*/
                + " OR MFA_CV_CTM SUBRANGE (210 CAST INTEGER) = 1"
                + ")";

        System.out.println("Start dumping "+mysqlTable+" table");
        
        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(sqlSetectOthers)){
                ResultSetMetaData metaResult = result.getMetaData();
                int numberOfColumns = metaResult.getColumnCount();
                makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
            } catch (SQLException e){e.printStackTrace();}
            System.out.println("Time elapsed: "+countTime(time)
                    +"\n----------------------------");
        
    }
    
    public void dumpModelsUA() {

        final String tableName = "TOF_MODELS";
        final String tableCountry = "TOF_COUNTRY_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_models_ua";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id INT(11) PRIMARY KEY, \n"
                + "manufacturer_id int(11), \n"
                + "description_id int(11), \n"
                + "start_date int(6), \n"
                + "end_date int(6), \n"
                + "passenger_car TINYINT, \n"
                + "commercial_vehicle TINYINT, \n"
                + "axle TINYINT, \n"
                + "description VARCHAR(255)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (manufacturer_id);\n";
        
        final String sqlSelectModels = "SELECT MOD_ID, MOD_MFA_ID, MOD_CDS_ID,"
                + " MOD_PCON_START, MOD_PCON_END, MOD_PC, MOD_CV, MOD_AXL, TEX_TEXT "
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " (MOD_PC_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " MOD_CV_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND"
                + " CDS_LNG_ID = " + RUSSIAN_ID + " AND CDS_TEX_ID = TEX_ID AND MOD_CDS_ID = CDS_ID"
                + " AND  CDS_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(sqlSelectModels)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
    
    public void dumpTypesUA() {
        
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_COUNTRY_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_ua";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "id int(11) PRIMARY KEY, \n" +
                "model_id int(11), \n" +
                "start_date int(6), \n" +
                "end_date int(6), \n" +
                "description varchar(100), \n" +
                "capacity float(5,1), \n" +
                "capacity_hp_from int(5), \n" +
                "capacity_kw_from int(5) \n" +
            ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        
        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (model_id);\n";
        
        final String selectTypes = "SELECT TYP_ID, TYP_MOD_ID,  TYP_PCON_START, TYP_PCON_END, TEX_TEXT, TYP_LITRES, TYP_HP_FROM, TYP_KW_FROM" +
                    " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE" +
                    " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR" +
                    " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND" +
                    " CDS_LNG_ID = " + RUSSIAN_ID + " AND CDS_TEX_ID = TEX_ID AND TYP_CDS_ID = CDS_ID" +
                    " AND  CDS_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1";
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectTypes)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    public void dumpArticlesUA() {

        final String tableName = "TOF_ARTICLES";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_articles_new_ua";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11), \n"
                + "article_nr VARCHAR(80), \n"
                + "supplier_id SMALLINT, \n" /*int(11)*/
                + "description VARCHAR(1024), \n"
                + "PRIMARY KEY (id) \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (supplier_id);\n" +
            "ALTER TABLE " + mysqlTable + " ADD INDEX (article_nr);\n";

        final String selectArticles = "SELECT ART_ID, ART_ARTICLE_NR, ART_SUP_ID, TEX_TEXT"
                    + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " "
                    + " WHERE ART_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 AND "
                    + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                    + " ART_COMPLETE_DES_ID = DES_ID ORDER BY ART_ID";
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectArticles)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }

    public void dumpSuppliersUA() {

        final String tableName = "TOF_SUPPLIERS";
        final String mysqlTable = "tof_suppliers_ua";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11), \n"
                + "brand VARCHAR(100), \n"
                + "alias VARCHAR(100), \n" /*same as previous*/
                + "supplier_nr SMALLINT, \n" /*int(11)*/
                + "PRIMARY KEY (id) \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (brand);\n" +
            "ALTER TABLE " + mysqlTable + " ADD INDEX (supplier_nr);\n";

        final String selectSuppliers = "SELECT DISTINCT SUP_ID, SUP_BRAND, SUP_BRAND, SUP_SUPPLIER_NR"
                    + " FROM " + tableName;
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectSuppliers)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
public void dumpSearchTree() {
        final String tableName = "TOF_SEARCH_TREE";
        final String mysqlTable = "tof_search_tree_ua";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "id INT(11), \n" +
                "parent_id INT(11), \n" +
                "type SMALLINT(2), \n" +
                "level SMALLINT(2), \n" +
                "sort INT(11), \n" + /*repaired*/
                "node_number INT(11), \n" +
                "text VARCHAR(255), \n" +
                "PRIMARY KEY (id) \n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (level);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (sort);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (type);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (parent_id);\n";
        
        final String selectSearchTree = "SELECT DISTINCT STR_ID, "
                    + "STR_ID_PARENT, STR_TYPE, STR_LEVEL, STR_SORT, "
                    + "STR_NODE_NR, TEX_TEXT" 
                    + " FROM " + tableName + ", " + tableCountry + ", " 
                    + tableDescriptions + " "
                    + " WHERE DES_LNG_ID = " + RUSSIAN_ID 
                    + " AND DES_TEX_ID = TEX_ID AND " 
                    + " DES_ID=STR_DES_ID";
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectSearchTree)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    public void dumpLinkGenericArticleSearchTree() {
        final String tableName = "TOF_LINK_GA_STR";
        final String mysqlTable = "tof_link_generic_article_search_tree_ua";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n" +
                "search_tree_id INT(11), \n" +
                "generic_article_id INT(11) \n" +
                ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";
        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (search_tree_id);\n" +
                "ALTER TABLE " + mysqlTable + " ADD INDEX (generic_article_id);\n";
        
        final String selectLinkGenericArticleSearchTree = "SELECT DISTINCT LGS_STR_ID, LGS_GA_ID" +
                    " FROM " + tableName;
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectLinkGenericArticleSearchTree)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
/**
 * Article parameters - not described in main runner - TexdocDump
 */
    public void dumpCriteria() {

        final String tableName = "TOF_CRITERIA";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_criteria";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "description varchar(255),\n"
                + "unit int(11),\n"
                + "type varchar(6),\n"
                + "is_interval int(5),\n"
                + "successor SMALLINT \n" /*int(11)*/
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (id);\n";

        final String selectCriteria = "SELECT CRI_ID, TEX_TEXT, CRI_UNIT_DES_ID, CRI_TYPE, CRI_IS_INTERVAL, CRI_SUCCESSOR"
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND CRI_SHORT_DES_ID = DES_ID ORDER BY CRI_ID";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectCriteria)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
 
    public void dumpArticlesAttributes() {

        final String tableName = "TOF_ARTICLE_INFO";
        final String tableCountry = "TOF_TEXT_MODULES";
        final String tableDescriptions = "TOF_TEXT_MODULE_TEXTS";
        final String mysqlTable = "tof_article_info";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "article_id int(11),\n"
                + "sort smallint,\n" /*int(11)*/
                + "description TEXT\n" /*what the FUCK???*/
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id);\n";
        
        final String selectArticleAttributes = "SELECT AIN_ART_ID, AIN_SORT, TMT_TEXT"
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " TMO_LNG_ID = " + RUSSIAN_ID + " AND TMO_TMT_ID = TMT_ID "
                + "AND AIN_TMO_ID = TMO_ID "
                + "ORDER BY AIN_ART_ID";
        
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectArticleAttributes)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }

    public void dumpTypesBody() {
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_body";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "body varchar(100)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String selectTupesBody = "SELECT TYP_ID, TEX_TEXT "
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND"
                + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                + "TYP_KV_BODY_DES_ID = DES_ID ORDER BY TYP_ID";

        final String sqlIndexes = "\n";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectTupesBody)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
            
    public void dumpTypesEngine() {
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_engine";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "engine varchar(100)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String selectTypesEngine = "SELECT TYP_ID, TEX_TEXT "
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND"
                + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                + "TYP_KV_ENGINE_DES_ID = DES_ID ORDER BY TYP_ID";

        final String sqlIndexes = "\n";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectTypesEngine)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
            
    public void dumpTypesFuel() {
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_fuel";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "fuel varchar(100)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String selectTypesFuel = "SELECT TYP_ID, TEX_TEXT "
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND"
                + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                + "TYP_KV_FUEL_DES_ID = DES_ID ORDER BY TYP_ID";

        final String sqlIndexes = "\n";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectTypesFuel)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }     

    public void dumpTypesDrive() {
        final String tableName = "TOF_TYPES";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_types_drive";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "drive varchar(100)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String selectTypesDrive = "SELECT TYP_ID, TEX_TEXT "
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " WHERE"
                + " (TYP_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " TYP_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1) AND"
                + " DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                + "TYP_KV_DRIVE_DES_ID = DES_ID ORDER BY TYP_ID";

        final String sqlIndexes = "\n";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectTypesDrive)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    public void dumpLinkTypeEngine() {
        final String tableName = "TOF_LINK_TYP_ENG";
        final String mysqlTable = "tof_link_type_engine";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "type_id int(11), \n"
                + "engine_id int(11) \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (type_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (engine_id);\n";

        final String selectLinkTypeEngine = "SELECT DISTINCT LTE_TYP_ID, LTE_ENG_ID "
                + " FROM " + tableName;

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectLinkTypeEngine)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
                
    public void dumpEngines() {
        final String tableName = "TOF_ENGINES";
        //final String tableCountry = "TOF_DESIGNATIONS";
        //final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_engines";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id int(11) PRIMARY KEY,\n"
                + "eng_code varchar(100) \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String selectEngines = "SELECT ENG_ID, ENG_CODE"
                + " FROM " + tableName + " WHERE"
                + " (ENG_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 OR"
                + " ENG_LA_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1)";

        final String sqlIndexes = "\n";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectEngines)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    public void dumpLinkTypeArticle() {
        final String tableName = "TOF_LINK_LA_TYP";
        final String mysqlTable = "tof_link_type_article";
        final String setCompressionVar = "SET GLOBAL innodb_file_per_table=1;\n"
                + "SET GLOBAL innodb_file_format=Barracuda;\n";
        final String initCompression = "ROW_FORMAT=COMPRESSED\n"
                + "KEY_BLOCK_SIZE=8\n";
        
        final String sqlCreateTable = setCompressionVar
                +" CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "type_id int(11), \n"
                + "article_link_id int(11), \n"
                + "generic_article_id int(11), \n"
                + "supplier_id  smallint\n" /*int(11)*/
                + ") " + initCompression
                + " ENGINE=InnoDB DEFAULT CHARSET=utf8;\n"; //default value  ENGINE=MYISAM

        final String sqlIndexes
                = "ALTER TABLE " + mysqlTable + " ADD INDEX (type_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (article_link_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (generic_article_id);\n";

        final String selectLinkTypeArticle = "SELECT DISTINCT LAT_TYP_ID, LAT_LA_ID, LAT_GA_ID, LAT_SUP_ID"
                + " FROM " + tableName + " WHERE LAT_CTM SUBRANGE (" + UKRAINE_CODE + " CAST INTEGER) = 1 AND LAT_TYP_ID >= 0";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectLinkTypeArticle)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }       

    public void dumpArticlesLink() {
        final String tableName = "TOF_LINK_ART";
        final String mysqlTable = "tof_article_link";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id INT(11), \n"
                + "article_id INT(11), \n"
                + "PRIMARY KEY (id)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n"; //default value  ENGINE=MYISAM

        final String sqlIndexes = "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id);\n";

        final String selectArticlesLink = "SELECT DISTINCT LA_ID, LA_ART_ID "
                + " FROM " + tableName;

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectArticlesLink)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    public void dumpGenericArticles() {
        final String tableName = "TOF_GENERIC_ARTICLES";
        final String mysqlTable = "tof_generic_articles";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "id INT(11), \n"
                + "description VARCHAR(255), \n"
                + "PRIMARY KEY (id)\n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes = "\n";

        final String selectGenericArticles = "SELECT DISTINCT GA_ID, TEX_TEXT"
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " "
                + " WHERE DES_LNG_ID = " + RUSSIAN_ID + " AND DES_TEX_ID = TEX_ID AND "
                + " DES_ID=GA_DES_ID";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectGenericArticles)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
        
    /**
     WHERE ACR_KV_DES_ID IS NULL
     */
    public void dumpCriteriaArticleIsNull() {
        final String tableName = "TOF_ARTICLE_CRITERIA";
        final String mysqlTable = "tof_article_criteria_is_null";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "article_id int(11),\n"
                + "sort tinyint,\n"
                + "criteria_id smallint,\n"
                + "value varchar(100),\n"
                + "display tinyint \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes
                = "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (criteria_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (sort);\n";

        final String selectCriteriaArticle = "SELECT ACR_ART_ID, ACR_SORT, ACR_CRI_ID, ACR_VALUE, ACR_DISPLAY"
                + " FROM " + tableName + " "
                + "WHERE ACR_KV_DES_ID IS NULL ORDER BY ACR_ART_ID";
          
        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectCriteriaArticle)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }
                    
    /**
     WHERE ACR_KV_DES_ID IS NOT NULL
     */
    public void dumpCriteriaArticleNotNull() {
        final String tableName = "TOF_ARTICLE_CRITERIA";
        final String tableCountry = "TOF_DESIGNATIONS";
        final String tableDescriptions = "TOF_DES_TEXTS";
        final String mysqlTable = "tof_article_criteria_not_null";

        final String sqlCreateTable = " CREATE TABLE IF NOT EXISTS " + mysqlTable + " (\n"
                + "article_id int(11),\n"
                + "sort tinyint,\n"
                + "criteria_id smallint,\n"
                + "description varchar(100),\n"
                + "display tinyint \n"
                + ") ENGINE=InnoDB DEFAULT CHARSET=utf8;\n";

        final String sqlIndexes
                = "ALTER TABLE " + mysqlTable + " ADD INDEX (article_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (criteria_id);\n"
                + "ALTER TABLE " + mysqlTable + " ADD INDEX (sort);\n";

        final String selectCriteriaArticle = "SELECT ACR_ART_ID, ACR_SORT, ACR_CRI_ID, TEX_TEXT, ACR_DISPLAY"
                + " FROM " + tableName + ", " + tableCountry + ", " + tableDescriptions + " "
                + "WHERE ACR_KV_DES_ID=DES_ID AND DES_TEX_ID=TEX_ID AND ACR_KV_DES_ID IS NOT NULL AND DES_LNG_ID=" + RUSSIAN_ID + "";

        System.out.println("Start dumping " + mysqlTable + " table");

        long time = System.currentTimeMillis();
        try (Statement st = connTransbase.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                ResultSet.CONCUR_READ_ONLY);
                ResultSet result = st.executeQuery(selectCriteriaArticle)) {
            ResultSetMetaData metaResult = result.getMetaData();
            int numberOfColumns = metaResult.getColumnCount();
            makeDump(result, numberOfColumns, mysqlTable, sqlCreateTable, sqlIndexes);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        System.out.println("Time elapsed: " + countTime(time)
                + "\n----------------------------");

    }

    
    //##############################################
    //##############################################
    //##############################################
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
        
        DumpFile df = null;
        try {
            df = new DumpFile(mysqlTable+".sql");
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        }
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
                            //case "CHAR":
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
                            case "NUMERIC":
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
                            case "BLOB":
                                if (rsTransbase.getObject(i) == null) {
                                    sb.append("NULL");
                                } else {
                                    Blob blob = rsTransbase.getBlob(i);
                                    byte[] bdata = blob.getBytes(1, (int)blob.length());
                                    String b2s = new String(bdata);
                                    b2s = b2s.replace("\u0000", "");
                                    b2s = b2s.replace(Character.toString((char)10), "");
                                    sb.append(b2s);
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
                    if(row % 100000 == 0){System.out.printf("%d rows added : %.2f%% %n",row,(((double)row)/rowCount*100));}
                }
                System.out.printf("%d rows added : %.2f%% %n",row,(((double)row)/rowCount*100));
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
