package com.forteanuncio.prep.dadospublicoscnpj.utils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.HashMap;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.annotations.MappedFieldFileWithColumnCassandra;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.GET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.firstUpperNameField;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.EMPTY;

public class CsvUtils<T> {
    
    private Class<?> clazz;
    
    public CsvUtils(Class<?> clazz){
        this.clazz = clazz;
    }

    public String generateHeaderByObject(Object emp) {
        try{

            if(clazz == null){
                @SuppressWarnings("unchecked")
                Class<?> clazzz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
                this.clazz = clazzz;
            }
            
            Field[] camposDaClasse = clazz.getDeclaredFields();
            Map<Integer,String> valores = new HashMap<Integer,String>();

            for(int i=0; i < camposDaClasse.length; i++){
                Field f = camposDaClasse[i];
                int valuePositionAnnotation = -1;
                if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                    Method metodo = clazz.getDeclaredMethod(GET_METHOD+firstUpperNameField(f));
                    if(metodo.invoke(emp) != null && !EMPTY.equals(metodo.invoke(emp).toString())){
                        valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                        valores.put(valuePositionAnnotation, f.getName());
                    }
                }
            }

            String header="";

            for(Map.Entry<Integer, String> entry : valores.entrySet()){
                header = header.concat(entry.getValue()).concat(",");
            }
            return header.substring(0, header.length()-1);
        }catch(IllegalArgumentException | IllegalAccessException | NoSuchMethodException | SecurityException | InvocationTargetException e){
            e.printStackTrace();
            return null;
        }

    }

}