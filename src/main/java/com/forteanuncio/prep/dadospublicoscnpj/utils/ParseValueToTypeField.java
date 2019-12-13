package com.forteanuncio.prep.dadospublicoscnpj.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

import com.datastax.driver.core.LocalDate;
import com.forteanuncio.prep.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;

import org.apache.logging.log4j.util.Strings;

public class ParseValueToTypeField {

    public static String SET_METHOD = "set";
    public static String GET_METHOD = "get";

    
    public static String convertToCsv(Object obj) throws IllegalArgumentException, IllegalAccessException, NoSuchMethodException, SecurityException, InvocationTargetException {
        Class<?> clazz = obj.getClass();
        Field[] camposDaClasse = clazz.getDeclaredFields();
        Map<Integer,String> valores = new HashMap<Integer,String>();

        for(int i=0; i < camposDaClasse.length; i++){
            Field f = camposDaClasse[i];
            int valuePositionAnnotation = -1;
            if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                Method m = clazz.getDeclaredMethod(GET_METHOD+firstUpperNameField(f.getName()));
                Object retValue = m.invoke(obj);
                valores.put(valuePositionAnnotation, retValue == null ? "" : retValue.toString());
            }
        }

        String meuArr[] = new String[valores.size()];
        valores.forEach((key, value) -> {
            meuArr[(key-1)] = value;
        });
        
        String ret = String.join("{", meuArr);
        return ret;
    }
    
    public static Object convertToObject(String emp, Class<?> clazz) throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, ParseException, InstantiationException {
        Object model = clazz.newInstance();
        String[] camposFile = emp.split("\",\"");
        Field[] camposClasses = clazz.getDeclaredFields();
        
        for(int i=0; i < camposClasses.length; i++){
            
            Field f = camposClasses[i];
            int valuePositionAnnotation = -1;
            
            Method metodoSet = null;
            
            if(f.getType().getSimpleName().equals("LocalDateTime")){
                metodoSet = model.getClass().getDeclaredMethod(SET_METHOD+firstUpperNameField(f.getName()), f.getType());
                metodoSet.invoke(model, LocalDateTime.now());
                continue;
            }else{
                if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                    valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                    valuePositionAnnotation--;
                    camposFile[valuePositionAnnotation] = camposFile[valuePositionAnnotation].replaceAll("\"","");
                    metodoSet = model.getClass().getDeclaredMethod(SET_METHOD+firstUpperNameField(f.getName()), f.getType());

                    switch(f.getType().getSimpleName()){
                        case "Long":
                            if(!Strings.EMPTY.equals(camposFile[valuePositionAnnotation])){
                                metodoSet.invoke(model, Long.parseLong(camposFile[valuePositionAnnotation]));
                            }
                            break;
                        case "Integer":
                            String value = camposFile[valuePositionAnnotation].replaceAll("[^0-9]", "");
                            if(!Strings.EMPTY.equals(value)){
                                metodoSet.invoke(model, Integer.parseInt(value));
                            }
                            break;
                        case "LocalDate":
                            if(camposFile[valuePositionAnnotation].length() == 8){
                                int year = Integer.parseInt(camposFile[valuePositionAnnotation].substring(0,4));
                                int month = Integer.parseInt(camposFile[valuePositionAnnotation].substring(4,6));
                                int day = Integer.parseInt(camposFile[valuePositionAnnotation].substring(6,8));
                                if(year > 0 && (month > 0 && month < 13) && (day > 0 && day < 32)){
                                    metodoSet.invoke(model, LocalDate.fromYearMonthDay(year, month, day));
                                }
                            }
                            break;
                        case "Float":
                            if(!Strings.EMPTY.equals(camposFile[valuePositionAnnotation])){
                                metodoSet.invoke(model, Float.parseFloat(camposFile[valuePositionAnnotation]));
                            }
                            break;
                        default:
                            metodoSet.invoke(model, camposFile[valuePositionAnnotation]);
                            break;
                    }
                }
            }
        }
        return model;
    }
    
    public static String firstUpperNameField(String field){
        return field.substring(0,1).toUpperCase()+field.substring(1);
    }

}