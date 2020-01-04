package com.forteanuncio.prep.dadospublicoscnpj.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;

import org.apache.logging.log4j.util.Strings;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.GET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.firstUpperNameField;

public class CsvUtils<T> {

    public String generateHeaderByObject(Object emp) {
        try{
            Class<?> clazz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
            Field[] camposDaClasse = clazz.getDeclaredFields();
            Map<Integer,String> valores = new HashMap<Integer,String>();

            for(int i=0; i < camposDaClasse.length; i++){
                Field f = camposDaClasse[i];
                int valuePositionAnnotation = -1;
                if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                    Method metodo = clazz.getDeclaredMethod(GET_METHOD+firstUpperNameField(f.getName()));
                    if(metodo.invoke(emp) != null && Strings.isNotEmpty(metodo.invoke(emp).toString())){
                        valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                        valores.put(valuePositionAnnotation, f.getName().toLowerCase());
                    }
                }
            }
            String meuArr[] = new String[valores.size()];
            List<Integer> colecao = new ArrayList<Integer>(valores.keySet());
            Collections.sort(colecao);
            for(int i =0; i < colecao.size(); i++){
                meuArr[i] = valores.get(colecao.get(i));
            }
            
            return String.join(",", meuArr);
        }catch(IllegalArgumentException | IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException e){
            e.printStackTrace();
            return null;
        }

    }

}