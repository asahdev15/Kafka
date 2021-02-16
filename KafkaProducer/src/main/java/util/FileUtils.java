package util;

import jdk.nashorn.internal.ir.debug.ObjectSizeCalculator;

import java.io.File;
import java.io.IOException;
import java.util.Date;
import java.util.Scanner;

public class FileUtils {

    public static void main(String[] args){
        System.out.println(new Date(1573184555455L));
    }

    public static long getObjectSize(Object input){
        long objSize = ObjectSizeCalculator.getObjectSize(input);
        System.out.println(objSize);
        return objSize;
    }

   public static String readFile(String fileName) {
      StringBuilder result = new StringBuilder("");
      File file = getFile(fileName);
      try (Scanner scanner = new Scanner(file)) {
          while (scanner.hasNextLine()) {
              String line = scanner.nextLine();
              result.append(line).append("\n");
          }
          scanner.close();
      } catch (IOException e) {
          e.printStackTrace();
      }
      return result.toString();
    }

   public static File getFile(String fileName) {
      ClassLoader classLoader = FileUtils.class.getClassLoader();
      File file = new File(classLoader.getResource(fileName).getFile());
      return file;
   }

}