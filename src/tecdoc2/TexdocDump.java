/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tecdoc2;

/**
 *
 * @author coder007
 */
public class TexdocDump {
    public static void main(String[] args) {
        DumpDB ddb = new DumpDB();
        
        // Cars
        //ddb.dumpManufacturersUA();//done
        //ddb.dumpManufacturersOther();//done
        ddb.dumpModelsUA();//done
        ddb.dumpTypesUA();//done
        
        //-----TO DO------
        
        ddb.dumpTypesBody();
        ddb.dumpTypesEngine();
        ddb.dumpTypesFuel();
        ddb.dumpTypesDrive();
        ddb.dumpLinkTypeEngine();
        ddb.dumpEngines();
            
        // Articles
        ddb.dumpArticlesUA();//done
        ddb.dumpSuppliersUA();//done
        ddb.dumpSearchTree();//done
        ddb.dumpLinkGenericArticleSearchTree();//done
        
        //CHAR to VARCHAR!
        /*
        ddb.dumpCriteria();//done - compare with dumpCriteriaArticle()!
        */
        //ddb.dumpArticlesLookupUA();//done
        
        //-----TO DO------
        
        ddb.dumpLinkTypeArticle();
        ddb.dumpArticlesLink();
        ddb.dumpGenericArticles();
        ddb.dumpArticlesAttributes();//done
        //check select statement precisely!
        ddb.dumpCriteriaArticleIsNull();
        ddb.dumpCriteriaArticleNotNull();
       
        //ddb.exportPictures();
        
        ddb.closeTransbase();
    }
}
