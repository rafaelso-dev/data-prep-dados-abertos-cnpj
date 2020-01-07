package com.forteanuncio.prep.dadospublicoscnpj.managers.mappers;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.lang.reflect.ParameterizedType;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.converters.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import com.forteanuncio.prep.dadospublicoscnpj.executors.mappers.MapperExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperManager<T> implements Runnable{
    
    public MapperManager() { }

    public ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(3);

    private static final Logger logger = LoggerFactory.getLogger(MapperManager.class);

    private CsvConverter<T> csvConverter;
    private CsvUtils<T> csvUtils;
    private SSTableConverter<T> ssTableConverter;
    
    @Override
    public void run() {
        try {
            logger.info("Starting Mapper manager.");

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
                    logger.error("Exist Readers running but not there nothing yet on list. waiting this Thread for 10ms");
                    Thread.sleep(1000);
                }
            }
            logger.info("Finishing Mapper manager.");
            executors.shutdown();
            System.out.println("Qtd of keys on map - "+Application.mapManaged.keySet().size());
            Application.existsMappers = false;
        } catch (Exception e) {
            logger.error("Erro on Map Manager. Details {}", e.getMessage());
        }
        
    }
    
    

}