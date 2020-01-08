package com.forteanuncio.prep.dadospublicoscnpj.executors.readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import com.forteanuncio.prep.dadospublicoscnpj.Application;
import com.forteanuncio.prep.dadospublicoscnpj.managers.readers.ReaderManager;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ReaderExecutor implements Runnable {

    private String pathDirectoryReader;
    private Integer bufferSize;

    private static final Logger logger = LoggerFactory.getLogger(ReaderExecutor.class);

    public ReaderExecutor(String pathDirectoryReader, int bufferSize) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.bufferSize = bufferSize;
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
            try{
                while((line = br.readLine()) != null){
                    Application.addItemOnListLinesManaged(line);
                    ReaderManager.addQtdLinesRead();
                }
                br.close();
                logger.debug("File {} loaded on memory.",arquivo.getName());
            }catch(final Exception e){
                e.printStackTrace();
            }
        } catch (final IOException e1) {
            e1.printStackTrace();
        }
        logger.debug("Finishing Reader Executor.");
    }

}