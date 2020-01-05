package com.forteanuncio.prep.dadospublicoscnpj.executors.mappers;

import java.lang.reflect.InvocationTargetException;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.converters.CsvConverter;
import com.forteanuncio.prep.dadospublicoscnpj.converters.SSTableConverter;
import com.forteanuncio.prep.dadospublicoscnpj.utils.CsvUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperExecutor<T> implements Runnable {

    private String line;
    private Class<?> clazz;

    private static final Logger logger = LoggerFactory.getLogger(MapperExecutor.class);
    
    CsvConverter<T> csvConverter;
    CsvUtils<T> csvUtils;
    SSTableConverter<T> ssTableConverter;

    public MapperExecutor(String line, Class<?> clazz) {
        this.line = line;
        this.clazz = clazz;
    }

    @Override
    public void run() {
        
        Object object = null;
        try {
            csvConverter = new CsvConverter<T>(clazz) {};
            csvUtils = new CsvUtils<T>(clazz) {};
            ssTableConverter = new SSTableConverter<T>(){};

            object = csvConverter.convertToObject(line);
            String genericHeader = csvUtils.generateHeaderByObject(object);
            List<Object> lineList = ssTableConverter.convertLineToListObjectTypes(object, genericHeader);

            if(Application.mapManaged.get(genericHeader) == null){
                List<List<Object>> list = new ArrayList<List<Object>>();
                list.add(lineList);
                Application.addListWithKeyOnMapManaged(genericHeader, list);
            }else {
                Application.addItemOnListWitKeyOnMapManaged(genericHeader, lineList);
            }
        } catch (NoSuchMethodException | SecurityException | IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException | ParseException e) {
            logger.error("Error on conversion. Details : {}", e.getMessage());
        }
    }

}