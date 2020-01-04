package com.forteanuncio.prep.dadospublicoscnpj.converter;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.text.ParseException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.datastax.driver.core.LocalDate;
import com.forteanuncio.prep.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.GET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.SET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.firstUpperNameField;

public class CsvConverter<T> {

    private static final Logger logger = LoggerFactory.getLogger(CsvConverter.class);
    public String convertToCsv(Object obj) {
        try{
            Class<?> clazz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
            Field[] camposDaClasse = clazz.getDeclaredFields();
            Map<Integer,String> valores = new HashMap<Integer,String>();

            for(int i=0; i < camposDaClasse.length; i++){
                Field f = camposDaClasse[i];
                int valuePositionAnnotation = -1;
                if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                    valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                    Method m = clazz.getDeclaredMethod(GET_METHOD+firstUpperNameField(f.getName()));
                    Object retValue = m.invoke(obj);
                    if(retValue != null && Strings.isNotEmpty(retValue.toString())){
                        valores.put(valuePositionAnnotation, retValue.toString());
                    }
                }
            }
            String meuArr[] = new String[valores.size()];
            List<Integer> colecao = new ArrayList<Integer>(valores.keySet());
            Collections.sort(colecao);
            for(int i =0; i < colecao.size(); i++){
                meuArr[i] = valores.get(colecao.get(i));
            }
            
            return String.join("{", meuArr);
        }catch(NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e){
            logger.error("Deu erro na conversão do registro ");
            e.printStackTrace();
            return null;
        }
        
    }

    public T convertToObject(String line) throws NoSuchMethodException, SecurityException, IllegalAccessException,
            IllegalArgumentException, InvocationTargetException, ParseException, InstantiationException {
        Class<T> clazz = (Class<T>) ((ParameterizedType) this.getClass().getGenericSuperclass()).getActualTypeArguments()[0];
        T model = clazz.newInstance();
        line = line.trim();
        String[] camposFile = line.split("\",\"");
        Field[] camposClasses = model.getClass().getDeclaredFields();
        
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

}