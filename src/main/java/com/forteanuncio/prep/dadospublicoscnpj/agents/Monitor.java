package com.forteanuncio.prep.dadospublicoscnpj.agents;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.managers.mappers.MapperManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;
import com.forteanuncio.prep.dadospublicoscnpj.managers.writers.WriterManager;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Monitor<T> implements Runnable{

    private Map<String, String> properties;

    public Monitor(Map<String, String> properties){
        this.properties = properties;
    }

    private static final Logger logger = LoggerFactory.getLogger(Monitor.class);

    @Override
    @SuppressWarnings("")
    public void run() {
        Integer value = isNotNullAndIsNotEmpty(properties.get("time.monitor.milliseconds")) ? Integer.valueOf(properties.get("time.monitor.milliseconds")) : 1000; 
        while(Application.existWriters || Application.existsMappers || Application.existsReaders){
            try{
                logger.info("Exists {} Readers, {} Mappers and {} Writers processing. Total lines reads: {}, total lines mapped {}, and total lines writed {}.",
                    ReaderManager.executors.getActiveCount(),
                    MapperManager.executors.getActiveCount(),
                    WriterManager.executors.getActiveCount(),
                    ReaderManager.getLines(),
                    MapperManager.getLines(),
                    WriterManager.getLines()
                );
                Thread.sleep(value);
            }catch(InterruptedException e){
                logger.error("Error on MonitorManager. Details : {}, Cause : {}. Trace : {}.", e.getMessage(), e.getCause(), e.getStackTrace());
            }
        }
        try{
            Thread.currentThread().stop();
        }catch(Exception e){
            logger.error("Error on try stop thread of monitoring.");
        }
    }

}