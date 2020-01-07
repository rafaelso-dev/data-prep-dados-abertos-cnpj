package com.forteanuncio.prep.dadospublicoscnpj.utils;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;;

public class Utils<T> {

    public static String SET_METHOD = "set";
    public static String GET_METHOD = "get";
    public static String EMPTY = "";
    
    private static final Logger logger = LoggerFactory.getLogger(Utils.class);
    private static InputStream inputStream;
    
    public static String firstUpperNameField(Field field){
        return field.getName().substring(0,1).toUpperCase()+field.getName().substring(1);
    }
    
    public static String firstUpperNameField(String field){
        return field.substring(0,1).toUpperCase()+field.substring(1);
    }

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
                logger.error("Error on try close inputStream of Properties File");
            }
		}
		return properties;
	}

    public String[] getFieldsByType(Class<?> clazz){
        String[] colunsListOfType = null;
        switch(clazz.getSimpleName()) {
            case "Empresa":
                colunsListOfType = new String[]{"cnpj bigint", "razaosocial text", "datahorainsercao timestamp", "bairro text", "capitalsocial float","cep int", "cnaefiscal int", "codigomotivositualcadastral int", "codigomunicipio int","codigonaturezajuridica int", "codigopais text", "complemento text", "dataexclusaosimplesnacional date", "datainicioatividade date","dataopcaopelosimplesnacional date", "datasituacaocadastral date", "datasituacaoespecial date", "ddd1 int", "ddd2 int", "dddfax int", "desclogradouro text", "email text", "logradouro text", "matriz int", "municipio text", "nomecidadeexterior text", "nomefantasia text", "nomepais text", "numero text", "numfax int", "opcaopelomei text", "opcaopelosimplesnacional text", "porteempresa text", "qualificacaoresponsavel int", "situacaocadastral int", "situacaoespecial text", "telefone1 int", "telefone2 int", "uf text"};
                break;
            case "CNAESecundaria":
                colunsListOfType = new String[]{};
                break;
            case "Socio":
                colunsListOfType = new String[]{};
                break;
        }
        return colunsListOfType;
    }

    public static boolean isNotNullAndIsNotEmpty(String str){
        return str == null ? 
                false : 
                str.length() > 0; 
    }
}