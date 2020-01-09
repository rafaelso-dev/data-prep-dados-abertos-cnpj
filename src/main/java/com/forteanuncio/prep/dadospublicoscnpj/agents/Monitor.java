package com.forteanuncio.prep.dadospublicoscnpj.agents;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor<T> implements Runnable {

    private Map<String, String> properties;

    public Monitor(Map<String, String> properties){
        this.properties = properties;
    }

    private static final Logger logger = LoggerFactory.getLogger(Monitor.class);

    @Override
    @SuppressWarnings("")
    public void run() {
        Integer value = isNotNullAndIsNotEmpty(properties.get("time.monitor.milliseconds")) ? 
                Integer.valueOf(properties.get("time.monitor.milliseconds")) : 1000; 
        while(Application.existWriters || Application.existsMappers || Application.existsReaders){
            try{
                logger.info(String.format("Exists %d Readers, %d Mappers and %d Writers processing. Total lines reads: %d,".
                    concat(" total lines mapped %d, and total lines writed %d."),
                    ReaderManager.executors.getActiveCount(),
                    MapperManager.executors.getActiveCount(),
                    WriterManager.executors.getActiveCount(),
                    ReaderManager.getLinesTotal(),
                    MapperManager.getTotalLines(),
                    WriterManager.getLines()
                ));
                Thread.sleep(value);
            }catch(InterruptedException e){
                logger.error("Error on MonitorManager. Details : {}, Cause : {}. Trace : {}.", 
                    e.getMessage(), e.getCause(), e.getStackTrace());
            }
        }
    }

}