package com.forteanuncio.prep.dadospublicoscnpj.managers.mappers;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.lang.reflect.ParameterizedType;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.converters.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import com.forteanuncio.prep.dadospublicoscnpj.executors.mappers.MapperExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperManager<T> implements Runnable{
    
    private static final Logger logger = LoggerFactory.getLogger(MapperManager.class);
    private static int qtdLinesMapped;
    private static int qtdLinesMappedTotal;

    private int threadPoolSize;
    private CsvConverter<T> csvConverter;
    private CsvUtils<T> csvUtils;
    private SSTableConverter<T> ssTableConverter;
    private static int maxSizeBatch;

    public static ThreadPoolExecutor executors;

    public MapperManager(Map<String, String> properties) {
        this.threadPoolSize = isNotNullAndIsNotEmpty(properties.get("threadpool.size.mappers.executors")) ? Integer.valueOf(properties.get("threadpool.size.mappers.executors")) : 1;
        maxSizeBatch = isNotNullAndIsNotEmpty(properties.get("mappers.maxSizeBatchPurge")) ? Integer.valueOf(properties.get("mappers.maxSizeBatchPurge")) : 2000;
    }
    
    @Override
    public void run() {
        try {
            executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
            logger.debug("Starting Mapper manager.");

            @SuppressWarnings("unchecked")
            Class<?> clazz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
            
            csvConverter = new CsvConverter<T>(clazz) {};
            csvUtils = new CsvUtils<T>(clazz) {};
            ssTableConverter = new SSTableConverter<T>(){};

            while(Application.listLinesManaged.size() > 0 || 
                qtdLinesMapped != ReaderManager.getLinesControlled() ||
                Application.existsReaders || 
                executors.getActiveCount() > 0) {
                    
                    if(executors.getActiveCount() == 0 && 
                        ReaderManager.readersBlocked && 
                        qtdLinesMapped == ReaderManager.getLinesControlled()){
                            ReaderManager.resetQtdLinesControlled();
                    }else if(ReaderManager.getLinesControlled() > 0 && executors.getActiveCount() < threadPoolSize){
                        String line = Application.removeFirstItemFromListLinesManaged();
                        if(line != null){
                            executors.execute(new MapperExecutor(line, csvConverter, csvUtils, ssTableConverter, maxSizeBatch));                    
                        }
                    }else{
                        Thread.sleep(500);
                    }
            }

            System.out.println("Finishing Mapper manager.");
            executors.shutdown();
            Application.existsMappers = false;
        } catch (Exception e) {
            logger.error("Erro on Map Manager. Details {}, Cause : {}, Trace {}", e.getMessage(), e.getCause(), e.getStackTrace());
        }
        
    }
    
    public static synchronized void resetQtdLinesMapped(){
        qtdLinesMapped = 0;
    }
    public static synchronized void addQtdLinesMapped(){
        qtdLinesMapped += 1;
        qtdLinesMappedTotal += 1;
    }
    public synchronized static int getLines(){
        return qtdLinesMapped;
    }
    
    public static synchronized int getTotalLines(){
        return qtdLinesMappedTotal;
    }

    public static synchronized int getMaxSizeBatch(){
        return maxSizeBatch;
    }

}