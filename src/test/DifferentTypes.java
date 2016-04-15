/*
 * 
 */
package test;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author user
 */
public class DifferentTypes {
    public static void main(String[] args) {
        List<Object> container = new ArrayList<>();
        container.add("String");
        container.add((byte)105);//Byte
        container.add(320000);//int
        container.add(32000000000000L);//long
        container.add((float)3.14);//float
        
        for(Object obj : container){
            //switch(obj.getClass().getName()){
            
            //}
            System.out.println(obj.getClass().getCanonicalName() +" : " + obj.toString());
        }
    }
}
