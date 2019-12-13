package com.forteanuncio.prep.dadospublicoscnpj.service.generator;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.forteanuncio.prep.dadospublicoscnpj.annotation.MappedFieldFileWithColumnCassandra;
import com.forteanuncio.prep.dadospublicoscnpj.model.Empresa;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.GeneratorFileDelimitedEmpresaExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.service.reader.ReaderEmpresa;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.ParseValueToTypeField.GET_METHOD;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.ParseValueToTypeField.firstUpperNameField;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GeneratorFileDelimitedEmpresa {

    @Value("${pasta.escrita.empresas}")
    private String pathDirectoryWriter;

    private static final String GENERIC_FILE = "GenericFile";

    public void generateFiles() {
        
        Set<String> listaHeaders = ReaderEmpresa.listaObjetosByHeader.keySet();
        for(String headerArquivo : listaHeaders){
            List<String> conteudoArquivo = ReaderEmpresa.listaObjetosByHeader.get(headerArquivo);
            Thread t = new Thread(new GeneratorFileDelimitedEmpresaExecutor(pathDirectoryWriter,headerArquivo,conteudoArquivo), GENERIC_FILE);
            t.start();
        }

    }

    public static String convertToCsvByObject(Empresa emp) {
        try{
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
            
            return String.join("{", meuArr);
        }catch(NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e){
            e.printStackTrace();
            return null;
        }
        
    }

    public static String generateHeaderByObject(Empresa emp) {
        try{
            Class<Empresa> clazz = Empresa.class;
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