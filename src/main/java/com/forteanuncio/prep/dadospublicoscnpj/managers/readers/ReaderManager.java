package com.forteanuncio.prep.dadospublicoscnpj.managers.readers;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.readers.ReaderExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderManager<T> implements Runnable {

    private String pathDirectoryReader;

    public ThreadPoolExecutor executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(2);

    private static final Logger logger = LoggerFactory.getLogger(ReaderManager.class);

    public ReaderManager(String pathDirectoryReader){
        this.pathDirectoryReader = pathDirectoryReader;
    }
    
    @Override
    public void run() {
        logger.info("Initializing ReaderManager");
        File path = new File(pathDirectoryReader);
        try {

            if (!path.exists() || !path.isDirectory()) {
                throw new IOException(String.format("The target specified '{}' is not a directory", pathDirectoryReader));
            }

            File[] arquivos = path.listFiles();

            //100MB of buffer
            int bufferSize = 104857600;
            
            for (File arquivo : arquivos) {
                executors.execute(new ReaderExecutor<T>(pathDirectoryReader+arquivo.getName(),bufferSize));
            }

            while(executors.getActiveCount() > 0){
                Thread.sleep(50);
            }
            executors.shutdown();
            Application.existsReaders = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.info("Finish ReaderManager");
    }

}