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
        //container.add(null);
        container.add("String");
        container.add((byte)105);//Byte
        container.add(320000);//int
        container.add((long)32000000);//long
        container.add((float)-99);//float
        
        for(Object obj : container){
            //switch(obj.getClass().getName()){
            
            //}
            System.out.println(obj.getClass().getSimpleName() +" : " + obj.toString());
        }
        if((float)container.get(4)==-99){
            System.out.println("TRUE");
        }
    }
}
