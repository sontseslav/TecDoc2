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
        //ddb.dumpModelsUA();//done
        //ddb.dumpTypesUA();//done
        
        //-----TO DO------
        
        //ddb.dumpTypesBody();//done
        //ddb.dumpTypesEngine();//done
        //ddb.dumpTypesFuel();//done
        //ddb.dumpTypesDrive();//done
        //ddb.dumpLinkTypeEngine();//done
        //ddb.dumpEngines();//done
            
        // Articles
        //ddb.dumpArticlesUA();//done
        //ddb.dumpSuppliersUA();//done
        //ddb.dumpSearchTree();//done
        //ddb.dumpLinkGenericArticleSearchTree();//done
        
        //CHAR to VARCHAR!
        /*
        ddb.dumpCriteria();//done
        */
        //ddb.dumpArticlesLookupUA();//done
        
        //-----TO DO------
        
        //ddb.dumpLinkTypeArticle();//done 105 mln
        //ddb.dumpArticlesLink();//done
        //ddb.dumpGenericArticles();//
        ddb.dumpArticlesAttributes();//done
        //check select statement precisely!
        //ddb.dumpCriteriaArticleIsNull();//done
        //ddb.dumpCriteriaArticleNotNull();//done
       
        //ddb.exportPictures();
        
        ddb.closeTransbase();
    }
}
