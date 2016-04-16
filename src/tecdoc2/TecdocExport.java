/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tecdoc2;

    /**
    Author: Leonid Yaremchuk
    Date: 2012-09-10
    Company: http://leonid.pro/
    */
     
    // run java -classpath tbjdbc.jar:mysql-connector-java-5.1.20-bin.jar:. TecdocExport
     
    public class TecdocExport {
     
        public static void main(String[] args) {
            ExportHelper1 helper = new ExportHelper1(new MysqlHelper().getConnection());
            //helper.printSysTable();
            
            // Cars
            //helper.exportManufacturersUA();//done
            //helper.exportManufacturersOther();//done
            //helper.exportModelsUA();//done
            //helper.exportModelsOther(); //errors?
            //helper.exportTypesUA();//done
            //helper.exportTypesBody();
            //helper.exportTypesEngine();
            //helper.exportTypesFuel();
            //helper.exportTypesDrive();
            //helper.exportLinkTypeEngine();
            //helper.exportEngines();
            
            // Articles
            //helper.exportArticlesUA(); //40min execution - done!
            //helper.exportSuppliersUA();//done
            //helper.exportArticlesLookupUA(); //Long time to perform - later!
            //helper.exportLinkTypeArticle();
            //helper.exportArticlesLink();
            //helper.exportGenericArticles();
            //helper.exportLinkGenericArticleSearchTree(); //dublicate
            //helper.exportArticlesAttributes();
            //helper.exportSearchTree();//done
            //helper.exportLinkGenericArticleSearchTree();//done
            //helper.exportCriteriaArticle();    
            //helper.exportPictures();
            helper.closeConnection();
        }
     
    }