package com.forteanuncio.prep.dadospublicoscnpj.utils;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class Utils<T> {

    public static String SET_METHOD = "set";
    public static String GET_METHOD = "get";
    public static String EMPTY = "";
    
    private static InputStream inputStream;
    
    public static Map<String, String> loadProperties() {
 
        Map<String,String> properties = new HashMap<String,String>();
		try {
            Properties props = new Properties();
			String propFileName = "application.properties";
 
			inputStream = Utils.class.getClassLoader().getResourceAsStream(propFileName);
 
			if (inputStream != null) {
				props.load(inputStream);
			} else {
				throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
			}
 
            props.forEach((key,value) -> {
                properties.put(String.valueOf(key), String.valueOf(value));
            });
 
		} catch (Exception e) {
			System.out.println("Exception: " + e);
		} finally {
            try{
                inputStream.close();
            }catch(IOException e){
                System.out.println("Error on try close inputStream of Properties File");
            }
		}
		return properties;
	}

    public static boolean isNotNullAndIsNotEmpty(String str){
        return str == null ? 
                false : 
                str.length() > 0; 
    }

    // private static void readFileTemporaryToCheckContent() throws IOException{
	// 	BufferedReader br = new BufferedReader(new FileReader(new File("/mnt/2f5d0b56-cec8-4246-b2ce-8307eda766c3/dados_receita_federal/socios/socios.csv")));
	// 	//int qtdLines = 0;
	// 	while(br.ready()){
	// 		String linha = br.readLine();
	// 		if(linha.contains("CONSTRUCAO E COMERCIO LTDA")){
	// 			System.out.println(linha);
	// 		}
			
	// 		//qtdLines++;
	// 	}
	// 	br.close();
	// }

}