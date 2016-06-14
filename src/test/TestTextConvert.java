/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 *
 * @author coder007
 */
public class TestTextConvert {
    private static final Map<Byte, Character> DICT = new Hashtable<>();
    
    
    public static void main(String[] args) throws UnsupportedEncodingException {
        byte[] b = {0x39,0x45,0x00,0x20,0x39,0x04,0x3E,0x04,0x31,0x04,0x20,0x4D,0x00};
        char[] ch = new char[b.length];
        initDict();
        for(int i=0, j=b.length; i < j; i++){
            ch[i] = (char) b[i];
            if (b[i] == 0x04){
                byte n = b[i-1];
                char chTemp = DICT.get(n);
                ch[i-1] = chTemp;
                ch[i] = 0x00;
            }
        }
        String s = new String(ch);
        s = s.replace("\u0000", "");
        System.out.println(s);
    }
    
    private static void initDict(){
        DICT.put((byte)0x01, 'Ё');
        DICT.put((byte)0x04, 'Є');
        DICT.put((byte)0x07, 'Ї');
        DICT.put((byte)0x06, 'І');
        DICT.put((byte)0x56, 'і');
        DICT.put((byte)0x51, 'ё');
        DICT.put((byte)0x54, 'є');
        DICT.put((byte)0x57, 'ї');
        
        DICT.put((byte)0x10, 'А');
        DICT.put((byte)0x11, 'Б');
        DICT.put((byte)0x12, 'В');
        DICT.put((byte)0x13, 'Г');
        DICT.put((byte)0x14, 'Д');
        DICT.put((byte)0x15, 'Е');
        DICT.put((byte)0x16, 'Ж');
        DICT.put((byte)0x17, 'З');
        DICT.put((byte)0x18, 'И');
        DICT.put((byte)0x19, 'Й');
        DICT.put((byte)0x1A, 'К');
        DICT.put((byte)0x1B, 'Л');
        DICT.put((byte)0x1C, 'М');
        DICT.put((byte)0x1D, 'Н');
        DICT.put((byte)0x1E, 'О');
        DICT.put((byte)0x1F, 'П');
        DICT.put((byte)0x20, 'Р');
        DICT.put((byte)0x21, 'С');
        DICT.put((byte)0x22, 'Т');
        DICT.put((byte)0x23, 'У');
        DICT.put((byte)0x24, 'Ф');
        DICT.put((byte)0x25, 'Х');
        DICT.put((byte)0x26, 'Ц');
        DICT.put((byte)0x27, 'Ч');
        DICT.put((byte)0x28, 'Ш');
        DICT.put((byte)0x29, 'Щ');
        DICT.put((byte)0x2A, 'Ъ');
        DICT.put((byte)0x2B, 'Ы');
        DICT.put((byte)0x2C, 'Ь');
        DICT.put((byte)0x2D, 'Э');
        DICT.put((byte)0x2E, 'Ю');
        DICT.put((byte)0x2F, 'Я');
        
        DICT.put((byte)0x30, 'а');
        DICT.put((byte)0x31, 'б');
        DICT.put((byte)0x32, 'в');
        DICT.put((byte)0x33, 'г');
        DICT.put((byte)0x34, 'д');
        DICT.put((byte)0x35, 'е');
        DICT.put((byte)0x36, 'ж');
        DICT.put((byte)0x37, 'з');
        DICT.put((byte)0x38, 'и');
        DICT.put((byte)0x39, 'й');
        DICT.put((byte)0x3A, 'к');
        DICT.put((byte)0x3B, 'л');
        DICT.put((byte)0x3C, 'м');
        DICT.put((byte)0x3D, 'н');
        DICT.put((byte)0x3E, 'о');
        DICT.put((byte)0x3F, 'п');
        DICT.put((byte)0x40, 'р');
        DICT.put((byte)0x41, 'с');
        DICT.put((byte)0x42, 'т');
        DICT.put((byte)0x43, 'у');
        DICT.put((byte)0x44, 'ф');
        DICT.put((byte)0x45, 'х');
        DICT.put((byte)0x46, 'ц');
        DICT.put((byte)0x47, 'ч');
        DICT.put((byte)0x48, 'ш');
        DICT.put((byte)0x49, 'щ');
        DICT.put((byte)0x4A, 'ъ');
        DICT.put((byte)0x4B, 'ы');
        DICT.put((byte)0x4C, 'ь');
        DICT.put((byte)0x4D, 'э');
        DICT.put((byte)0x4E, 'ю');
        DICT.put((byte)0x4F, 'я');
    }
}
