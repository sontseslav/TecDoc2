/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author coder007
 */
public class TestFileWrite {
    public static void main(String[] args) {
        String str = "My string;\n";
        try(OutputStream os = new FileOutputStream("testwrite.txt")){
            StringBuilder sb = new StringBuilder();
            os.write(str.getBytes());
            os.flush();
            System.out.println("done");
        } catch (FileNotFoundException ex) {
            ex.printStackTrace();
        } catch (IOException ex) {
            ex.printStackTrace();
        }
    }
}
