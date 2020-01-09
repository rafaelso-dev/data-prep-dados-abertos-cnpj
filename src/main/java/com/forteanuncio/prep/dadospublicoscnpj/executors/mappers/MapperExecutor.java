package com.forteanuncio.prep.dadospublicoscnpj.executors.mappers;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.converters.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperExecutor implements Runnable {

    private String line;
    
    private static final Logger logger = LoggerFactory.getLogger(MapperExecutor.class);
    
    private CsvConverter<?> csvConverter;
    private CsvUtils<?> csvUtils;
    private SSTableConverter<?> ssTableConverter;
    private int maxSizeBatch;

    public MapperExecutor(String line, CsvConverter<?> csvConverter, 
            CsvUtils<?> csvUtils, SSTableConverter<?> ssTableConverter,
            int maxSizeBatch) {
        this.line = line;
        this.csvConverter = csvConverter;
        this.csvUtils = csvUtils;
        this.ssTableConverter = ssTableConverter;
        this.maxSizeBatch = maxSizeBatch;
    }

    @Override
    public void run() {
        logger.debug("Starting Mapper Executor.");
        Object object = null;
        try {
            object = csvConverter.convertToObject(line);
            String key = csvUtils.generateKeyByObject(object);
            
            if(Application.listKeysBlocked.contains(key)){
                Application.addItemOnListLinesManaged(line);
            }else{
                List<Object> listColumnsLine = ssTableConverter.convertLineToListObjectTypes(object, key);

                if(Application.mapManaged.get(key) == null){
                    List<List<Object>> list = new CopyOnWriteArrayList<List<Object>>();
                    list.add(listColumnsLine);
                    Application.addListWithKeyOnMapManaged(key, list);
                }else {
                    Application.addItemOnListWitKeyOnMapManaged(key, listColumnsLine);
                }
                if(Application.mapManaged.get(key) != null && Application.mapManaged.get(key).size() >= maxSizeBatch){
                    Application.addKeyOnListKeysBlocked(key);
                }
            }
            
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | ParseException e) {
            logger.error("Error on conversion. Details : {}, Cause: {}, StackTrace : {}", e.getMessage(), e.getCause(), e.getStackTrace());
        }
        logger.debug("Finishing Mapper Executor.");
    }

}