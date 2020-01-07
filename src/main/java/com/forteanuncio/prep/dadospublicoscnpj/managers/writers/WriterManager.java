package com.forteanuncio.prep.dadospublicoscnpj.managers.writers;

import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.writers.WriterExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterManager<T> implements Runnable{

    private String pathDirectoryWriter;

    private static Logger logger = LoggerFactory.getLogger(WriterManager.class);

    public ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
    
    public WriterManager(String pathDirectoryWriter){
        this.pathDirectoryWriter = pathDirectoryWriter;
    }

    public static int qtdRows = 0;

    @Override
    public void run() {
        logger.info("Starting Writer Manager"); 

        try{
            
            while(Application.listKeysBlocked.size() > 0 || 
                Application.mapManaged.keySet().size() > 0 || 
                Application.existsMappers || 
                executors.getActiveCount() > 0){
                logger.info("Qtd of keys to writer {}", Application.mapManaged.keySet().size());
                
                if(Application.listKeysBlocked.size() > 0){
                    logger.debug("Iterating keys of mapManaged");
                    while(Application.listKeysBlocked.size() > 0){
                        if(executors.getActiveCount() < 20){
                            try{
                                String key = Application.removeFirstItemFromListKeysBlocked();
                                List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                                
                                logger.debug("passing {} lines to some Executor.", values.size());
                                executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                            }catch(IndexOutOfBoundsException e){
                                logger.info("Error on try remove key of listKeysBlocked.");
                            }
                        }
                    }
                    
                }else if(Application.mapManaged.keySet().size() > 0 && !Application.existsMappers && executors.getActiveCount() < 20){
                    try{
                        for(int i=0; i < 20-executors.getActiveCount() && Application.mapManaged.keySet().size() > 0; i++){
                            String key = Application.mapManaged.keySet().iterator().next();
                            List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                            logger.debug("passing {} lines to some Executor.", values.size());
                            executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                        }
                    }catch(Exception e){
                        logger.error("Error on Writer Manager. Details {}",e.getMessage());
                    }
                }else{
                    Thread.sleep(250);
                }
            }
            executors.shutdown();
            Application.existWriters = false;
        }catch(Exception e){
            logger.error("Error on writer Manager. Details: {}",e.getMessage());
        }
        logger.info("Finishing Writer Manger. Total lines processed {}", qtdRows); 
    }

    public synchronized static void addLines(int lines){
        qtdRows = qtdRows + lines;
    }
    
}