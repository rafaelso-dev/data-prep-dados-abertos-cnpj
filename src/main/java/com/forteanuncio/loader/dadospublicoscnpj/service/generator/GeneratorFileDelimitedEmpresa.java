package com.forteanuncio.loader.dadospublicoscnpj.service.generator;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import com.forteanuncio.loader.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;
import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;
import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.GET_METHOD;
import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.firstUpperNameField;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GeneratorFileDelimitedEmpresa implements GeneratorFileDelimited {

    @Value("${pasta.escrita.empresas}")
    private String pathDirectoryWriter;

    @Override
    public String addFile(List<String> lista, String genericHeader) {
        try {
            String fileName = Thread.currentThread().getName()+"."+UUID.randomUUID().toString();
            File f = new File(pathDirectoryWriter);
            if (!f.exists()) {
                f.mkdirs();
            }
            f = new File(pathDirectoryWriter+fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            bw.append(genericHeader).append(Strings.LINE_SEPARATOR);
            lista.forEach(line -> {
                try {
                    bw.append(line).append(Strings.LINE_SEPARATOR);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            bw.close();
            return fileName;
        } catch (IOException ex) {
            ex.printStackTrace();
        }
        return null;
    }

    public String convertToCsvByObject(Empresa emp) throws NoSuchMethodException, SecurityException,
            IllegalAccessException, IllegalArgumentException, InvocationTargetException {
        Class<?> clazz = emp.getClass();
        Field[] camposDaClasse = clazz.getDeclaredFields();
        Map<Integer,String> valores = new HashMap<Integer,String>();

        for(int i=0; i < camposDaClasse.length; i++){
            Field f = camposDaClasse[i];
            int valuePositionAnnotation = -1;
            if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                Method m = clazz.getDeclaredMethod(GET_METHOD+firstUpperNameField(f.getName()));
                Object retValue = m.invoke(emp);
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
        
        String ret = String.join("{", meuArr);
        return ret;
    }

    @Override
    public String generateHeaderByObject(Empresa emp) throws IllegalArgumentException, IllegalAccessException,
            NoSuchMethodException, SecurityException, InvocationTargetException {
        Class<Empresa> clazz = (Class<Empresa>) emp.getClass();
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
        
        String ret = String.join(",", meuArr);
        return ret;
    }

    @Deprecated
    public static String generateHeaderOfClass(Class<?> clazz){
        Field[] camposDaClasse = clazz.getDeclaredFields();
        Map<Integer,String> valores = new HashMap<Integer,String>();

        for(int i=0; i < camposDaClasse.length; i++){
            Field f = camposDaClasse[i];
            int valuePositionAnnotation = -1;
            if(f.isAnnotationPresent(MappedFieldFileWithColumnCassandra.class)){
                valuePositionAnnotation = f.getAnnotation(MappedFieldFileWithColumnCassandra.class).value();
                valores.put(valuePositionAnnotation, f.getName().toLowerCase());
            }
        }
        String meuArr[] = new String[valores.size()];
        valores.forEach((key, value) -> {
            meuArr[(key-1)] = value;
        });
        
        String ret = String.join(",", meuArr);
        return ret;
    }

}