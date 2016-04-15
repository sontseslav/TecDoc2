/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package test;

import java.util.Iterator;
import javax.imageio.ImageIO;
import javax.imageio.ImageReader;

/**
 *
 * @author coder007
 */
public class TestPic {
    public static void main(String[] args){
        Iterator<ImageReader> iter = ImageIO.getImageReadersByFormatName("JPEG2000");
        boolean isSupported = iter.hasNext();
        System.out.println(isSupported);
    }
}
