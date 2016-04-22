package tecdoc2;

    /**
    Author: Leonid Yaremchuk
    Date: 2012-09-10
    Company: http://leonid.pro/
    */
     
    // run java -classpath tbjdbc.jar:mysql-connector-java-5.1.20-bin.jar:. TecdocExport
     
    public class TecdocExport {
     
        public static void main(String[] args) {
            //ExportHelper helper = new ExportHelper(new MysqlHelper().getConnection());
            TecDocPreparator tdPrep = new TecDocPreparator(
                    new MysqlHelper().getConnection());
            //helper.printSysTable();
            //tdPrep.printSysTable();
            
            // Cars
            //tdPrep.exportManufacturersUA();//done
            //tdPrep.exportManufacturersUA();
            //tdPrep.exportManufacturersOther();//done
            //tdPrep.exportModelsUA();//done
            //helper.exportModelsOther(); //errors?
            //tdPrep.exportTypesUA();//done
            //helper.exportTypesBody();
            //helper.exportTypesEngine();
            //helper.exportTypesFuel();
            //helper.exportTypesDrive();
            //helper.exportLinkTypeEngine();
            //helper.exportEngines();
            
            // Articles
            tdPrep.exportArticlesUA(); //40min execution - done!
            tdPrep.exportSuppliersUA();//done
            //tdPrep.exportArticlesLookupUA(); //Long time to perform - later!
            //helper.exportLinkTypeArticle();
            //helper.exportArticlesLink();
            //helper.exportGenericArticles();
            //helper.exportLinkGenericArticleSearchTree(); //dublicate
            //helper.exportArticlesAttributes();
            //tdPrep.exportSearchTree();//done
            //tdPrep.exportLinkGenericArticleSearchTree();//done
            //helper.exportCriteriaArticle();    
            //helper.exportPictures();
            //helper.closeConnection();
            tdPrep.closeConnection();
        }
     
    }