package com.forteanuncio.prep.dadospublicoscnpj.managers.writers;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.lang.reflect.ParameterizedType;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.writers.WriterExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterManager<T> implements Runnable{

    private String pathDirectoryWriter;

    private static Logger logger = LoggerFactory.getLogger(WriterManager.class);

    public ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
    
    public WriterManager(String pathDirectoryWriter){
        this.pathDirectoryWriter = pathDirectoryWriter;
    }

    public static int qtdRows = 0;

    @Override
    public void run() {
        try{
            logger.info("Iniciando Gerador de arquivos"); 
            
            @SuppressWarnings("unchecked")
            Class<?> clazz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
            
            Set<String> mapas = Application.mapManaged.keySet();
            Iterator<String> iter = mapas.iterator();
            while(iter.hasNext()){
                while(executors.getActiveCount() == 10){
                    Thread.sleep(50);
                }
                String key = iter.next();
                List<List<Object>> conteudoArquivo = Application.mapManaged.get(key);
                executors.execute(new WriterExecutor<T>(pathDirectoryWriter,key,conteudoArquivo, clazz));
            }
            
            while(executors.getActiveCount() > 0){
                Thread.sleep(50);
            }
            executors.shutdown();

            mapas = Application.mapManaged.keySet();
            if(mapas.size() > 0 ){
                run();
            }
            logger.info("Finalizando Gerador de arquivos"); 
            logger.info("Total de linhas processadas até agora {}", qtdRows);
            
        }catch(Exception e){
            logger.error("Error on writer Manager. Details: {}",e.getMessage());
            
        }
    }

    public synchronized static void addLine(){
        qtdRows++;
    }
    
}