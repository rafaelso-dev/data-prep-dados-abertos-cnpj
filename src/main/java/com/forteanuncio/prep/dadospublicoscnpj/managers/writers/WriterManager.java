package com.forteanuncio.prep.dadospublicoscnpj.managers.writers;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.writers.WriterExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;

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
            Set<String> externalKeySet = Application.mapManaged.keySet();
            while(Application.listKeysBlocked.size() > 0 || 
                Application.mapManaged.keySet().size() > 0 || 
                Application.existsMappers || 
                qtdLinesWrited != MapperManager.getTotalLines() ||
                executors.getActiveCount() > 0){
                
                if(Application.listKeysBlocked.size() > 0){
                    logger.debug("Iterating keys blocked of mapManaged");
                    while(Application.listKeysBlocked.size() > 0){
                        if(executors.getActiveCount() < threadPoolSize){
                            try{
                                String key = Application.removeFirstItemFromListKeysBlocked();
                                List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                                if(values != null){
                                    executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                                }
                            }catch(IndexOutOfBoundsException e){
                                logger.error("Error on try remove key of listKeysBlocked.");
                            }
                        }
                    }
                    
                }
                if(externalKeySet.size() > 0 && 
                    !Application.existsMappers ){
                    try{
                        Iterator<String> iter = externalKeySet.iterator();
                        while(iter.hasNext()){
                            if((threadPoolSize-executors.getActiveCount()) > 0){
                                String key = iter.next();
                                List<List<Object>> values = Application.removeKeyFromMapManaged(key);
                                if(values != null){
                                    //int tam = values.size();
                                    //addLines(tam);
                                    //System.out.println("Lines sended to executor - "+tam + " Total = " +getLines());
                                    //logger.debug("passing {} lines to some Executor.", tam);
                                    executors.execute(new WriterExecutor(pathDirectoryWriter, key, values));
                                }
                            }
                        }
                    }catch(Exception e){
                        logger.error("Error on Writer Manager but still in loop. Details: {}. Cause: {}. Trace: {}.",
                            e.getMessage(), e.getCause(), e.getStackTrace());
                    }
                }

                Thread.sleep(500);
                externalKeySet = Application.mapManaged.keySet();
                
            }
            executors.shutdown();
            Application.existWriters = false;
        }catch(Exception e){
            logger.error("Error on writer Manager. Still out of loop, stopping application. Details {}. Cause {}. Trace {}.",e.getMessage(), e.getCause(), e.getStackTrace());
            System.exit(1);
        }
        System.out.println("Finishing Writer Manger. Total lines processed " + getLines()); 
    }

    public static synchronized void addLines(int linesWrited){
        qtdLinesWrited += linesWrited;
    }

    public static synchronized int getLines(){
        return qtdLinesWrited;
    }
    
}