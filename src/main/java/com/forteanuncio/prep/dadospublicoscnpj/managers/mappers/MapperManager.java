package com.forteanuncio.prep.dadospublicoscnpj.managers.mappers;

import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.lang.reflect.ParameterizedType;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.converters.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import com.forteanuncio.prep.dadospublicoscnpj.executors.mappers.MapperExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperManager<T> implements Runnable{
    
    public MapperManager(Map<String, String> properties) {
        this.threadPoolSize = isNotNullAndIsNotEmpty(properties.get("threadpool.size.readers.executors")) ? Integer.valueOf(properties.get("threadpool.size.readers.executors")) : 1;
    }

    public static ThreadPoolExecutor executors;
    private int threadPoolSize;
    private static final Logger logger = LoggerFactory.getLogger(MapperManager.class);
    private static int qtdLinesMapped;

    private CsvConverter<T> csvConverter;
    private CsvUtils<T> csvUtils;
    private SSTableConverter<T> ssTableConverter;
    
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

            while(Application.listLinesManaged.size() > 0 || Application.existsReaders || executors.getActiveCount() > 0) {
                try{
                    String line = Application.removeFirstItemFromListLinesManaged();
                    executors.execute(new MapperExecutor<T>(line, csvConverter, csvUtils, ssTableConverter));
                }catch(IndexOutOfBoundsException e){
                    // logger.error("Exist Readers running but not there nothing yet on list. waiting this Thread for 500ms");
                    Thread.sleep(500);
                }
            }
            logger.debug("Finishing Mapper manager.");
            executors.shutdown();
            Application.existsMappers = false;
        } catch (Exception e) {
            logger.error("Erro on Map Manager. Details {}, Cause : {}, Trace {}", e.getMessage(), e.getCause(), e.getStackTrace());
        }
        
    }
    
    public static synchronized void addQtdLinesMapped(){
        qtdLinesMapped += 1;
    }
    public synchronized static int getLines(){
        return qtdLinesMapped;
    }

}