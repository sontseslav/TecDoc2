/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package tecdoc2;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author coder007
 */
public class DumpFile {
    private static DumpFile instance;
    private final OutputStream os;
    //private boolean osClosed;
    private byte[] buffer;
    
    public DumpFile(String filename) throws FileNotFoundException{
        os = new FileOutputStream(filename);
    }
    
    public static DumpFile dumpFileFactory(String filename){
        if(instance == null){
            try{
                instance = new DumpFile(filename);
            }catch(FileNotFoundException e){
                e.printStackTrace();
                return null;
            }
        }
        return instance;
    }
    
    public void makeWrite(String text){
        buffer = text.getBytes();
        try{
            os.write(buffer);
        }catch(IOException e){e.printStackTrace();}
    }
    
    public void close(){
        try{
            os.flush();
            os.close();
        }catch(IOException e){e.printStackTrace();}
        //osClosed = true;
    }
    /*
    @Override
    public void finalize(){
        if(!osClosed){
            try{
                os.close();
            }catch(IOException e){e.printStackTrace();}
            osClosed = true;
        }
        try {
            super.finalize();
        } catch (Throwable ex) {
            ex.printStackTrace();
        }
    
    }*/
}
