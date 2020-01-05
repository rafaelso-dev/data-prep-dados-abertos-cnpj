package com.forteanuncio.prep.dadospublicoscnpj.managers.mappers;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import java.lang.reflect.ParameterizedType;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.mappers.MapperExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperManager<T> implements Runnable{
    
    public MapperManager() { }

    public ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);

    private static final Logger logger = LoggerFactory.getLogger(MapperManager.class);
    
    @Override
    public void run() {
        try {
            @SuppressWarnings("unchecked")
            Class<?> clazz = ((Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0]);
            
            logger.info("Starting executor mapper");
            while(Application.listManaged.size() > 0) {
                
                while(executors.getActiveCount() == 10){
                    Thread.sleep(20);
                }
                executors.execute(new MapperExecutor<T>(Application.removeFirstItemFromManaged(), clazz));
            }

            logger.info("Finishing executor mapper");
            
            while(executors.getActiveCount() > 0){
                Thread.sleep(5);
            }
            executors.shutdown();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    
    

}