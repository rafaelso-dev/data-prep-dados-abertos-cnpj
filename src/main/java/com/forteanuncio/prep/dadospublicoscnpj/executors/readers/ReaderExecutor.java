package com.forteanuncio.prep.dadospublicoscnpj.executors.readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ReaderExecutor implements Runnable {

    private String pathDirectoryReader;
    private int bufferSize;
    private int maxSizeList;

    private static final Logger logger = LoggerFactory.getLogger(ReaderExecutor.class);

    public ReaderExecutor(String pathDirectoryReader, int bufferSize, int maxSizeList) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.bufferSize = bufferSize;
        this.maxSizeList = maxSizeList;
    }

    @Override
    public void run() {
        
        logger.debug("Starting Reader Executor.");
        File arquivo = new File(pathDirectoryReader);
        
        BufferedReader br;
        logger.debug("Loading file {} to memory",arquivo.getName());
        try {
            br = new BufferedReader(new FileReader(arquivo), bufferSize);
            String line = null;
            while(br.ready()){
                if(!ReaderManager.readersBlocked){
                    if(ReaderManager.getLinesControlled() < maxSizeList){
                        line = br.readLine();
                        Application.addItemOnListLinesManaged(line);
                        ReaderManager.addQtdLinesRead();
                    }else{
                        ReaderManager.readersBlocked = true;
                    }
                }else{
                    Thread.sleep(500);
                }
            }
            br.close();
            logger.debug("File {} loaded on memory.",arquivo.getName());

        } catch (Exception e) {
            logger.error("Error on Reader Executor. Details: {}, Cause: {}, StackTrace {}", 
                    e.getMessage(), e.getCause(), e.getStackTrace());
        }
        logger.debug("Finishing Reader Executor.");
    }

}