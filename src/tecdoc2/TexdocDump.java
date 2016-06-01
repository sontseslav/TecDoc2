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
        //ddb.dumpManufacturersUA();
        //ddb.dumpArticlesLookupUA();
        ddb.dumpManufacturersOther();
        ddb.closeTransbase();
    }
}
