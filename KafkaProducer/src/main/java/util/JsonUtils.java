package util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.ObjectOutputStream;

public class JsonUtils {

    public static int getMessageSizeInBytes(Object message) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos);
        oos.writeObject(writeToJson(message));
        oos.close();
        return baos.size();
    }

    public static <T> T readObject(String jsonFile, Class<T> classType) throws Exception
    {
        ObjectMapper mapper = new ObjectMapper();
        T object = mapper.readValue(FileUtils.getFile(jsonFile), classType);
        return object;
    }

    public static String writeToJson(Object value){
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            return mapper.writeValueAsString(value);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void writeToFile(String filePath, Object obj){
        try {
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);
            mapper.writeValue(new File(filePath), obj);
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }
}
