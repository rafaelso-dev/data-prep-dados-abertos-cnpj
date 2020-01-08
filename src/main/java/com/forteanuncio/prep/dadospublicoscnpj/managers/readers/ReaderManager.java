package com.forteanuncio.prep.dadospublicoscnpj.managers.readers;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.executors.readers.ReaderExecutor;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReaderManager implements Runnable {

    private String pathDirectoryReader;

    private int threadPoolSize;
    
    public static ThreadPoolExecutor executors;

    private static final Logger logger = LoggerFactory.getLogger(ReaderManager.class);
    private static int qtdLinesRead;

    public ReaderManager(String pathDirectoryReader, Map<String, String> properties){
        this.threadPoolSize = isNotNullAndIsNotEmpty(properties.get("threadpool.size.mappers.executors")) ? Integer.valueOf(properties.get("threadpool.size.mappers.executors")) : 1;
        this.pathDirectoryReader = pathDirectoryReader;
    }
    
    @Override
    public void run() {
        logger.debug("Starting ReaderManager");
        executors = (ThreadPoolExecutor) Executors.newFixedThreadPool(threadPoolSize);
        File path = new File(pathDirectoryReader);
        try {

            if (!path.exists() || !path.isDirectory()) {
                logger.error("The target specified to reader files: {} is not a directory", pathDirectoryReader);
                throw new IOException();
            }

            File[] arquivos = path.listFiles();

            //10MB of buffer
            int bufferSize = 10485760;
            
            for (File arquivo : arquivos) {
                executors.execute(new ReaderExecutor(pathDirectoryReader+arquivo.getName(),bufferSize));
            }

            while(executors.getActiveCount() > 0){
                Thread.sleep(500);
            }
            executors.shutdown();
            Application.existsReaders = false;
        } catch (Exception e) {
            e.printStackTrace();
        }
        logger.debug("Finish ReaderManager");
    }

    public static synchronized void addQtdLinesRead(){
        qtdLinesRead += 1;
    }
    public synchronized static int getLines(){
        return qtdLinesRead;
    }

}