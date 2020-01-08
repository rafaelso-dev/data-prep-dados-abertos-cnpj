package com.forteanuncio.prep.dadospublicoscnpj.managers.writers;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.writers.WriterExecutor;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WriterManager<T> implements Runnable{

    private int threadPoolSize;
    private String pathDirectoryWriter;

    private static Logger logger = LoggerFactory.getLogger(WriterManager.class);
    private static int qtdLinesWrited = 0;

    public static ThreadPoolExecutor executors;
    
    public WriterManager(String pathDirectoryWriter, Map<String, String> properties){
        this.threadPoolSize = isNotNullAndIsNotEmpty(properties.get("threadpool.size.writers.executors")) ? Integer.valueOf(properties.get("threadpool.size.writers.executors")) : 1;
        this.pathDirectoryWriter = pathDirectoryWriter;
    }

    @Override
    public void run() {
        logger.debug("Starting Writer Manager");
        executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
        try{
            
            while(Application.listKeysBlocked.size() > 0 || 
                Application.mapManaged.keySet().size() > 0 || 
                Application.existsMappers || 
                executors.getActiveCount() > 0){
                
                if(Application.listKeysBlocked.size() > 0){
                    logger.debug("Iterating keys blocked of mapManaged");
                    while(Application.listKeysBlocked.size() > 0){
                        if(executors.getActiveCount() < threadPoolSize){
                            try{
                                String key = Application.removeFirstItemFromListKeysBlocked();
                                List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                                executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                            }catch(IndexOutOfBoundsException e){
                                logger.error("Error on try remove key of listKeysBlocked.");
                            }
                        }
                    }
                    
                }else if(Application.mapManaged.keySet().size() > 0 && !Application.existsMappers && executors.getActiveCount() < threadPoolSize){
                    try{
                        for(int i=0; i < threadPoolSize-executors.getActiveCount() && Application.mapManaged.keySet().size() > 0; i++){
                            String key = Application.mapManaged.keySet().iterator().next();
                            List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                            logger.debug("passing {} lines to some Executor.", values.size());
                            executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                        }
                    }catch(Exception e){
                        logger.error("Error on Writer Manager but still in loop. Details: {}. Cause: {}. Trace: {}.",e.getMessage(), e.getCause(), e.getStackTrace());
                    }
                }else{
                    Thread.sleep(500);
                }
            }
            executors.shutdown();
            Application.existWriters = false;
        }catch(Exception e){
            logger.error("Error on writer Manager. Still out of loop, stopping application. Details {}. Cause {}. Trace {}.",e.getMessage(), e.getCause(), e.getStackTrace());
            System.exit(1);
        }
        logger.debug("Finishing Writer Manger. Total lines processed {}", qtdLinesWrited); 
    }

    public synchronized static void addLines(int linesWrited){
        qtdLinesWrited += linesWrited;
    }
    public synchronized static int getLines(){
        return qtdLinesWrited;
    }
    
}