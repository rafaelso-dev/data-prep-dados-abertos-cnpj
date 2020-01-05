package com.forteanuncio.prep.dadospublicoscnpj.executors.readers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import com.forteanuncio.prep.dadospublicoscnpj.Application;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

public class ReaderExecutor<T> implements Runnable {

    private String pathDirectoryReader;
    private Integer bufferSize;

    private static final Logger logger = LoggerFactory.getLogger(ReaderExecutor.class);

    public ReaderExecutor(String pathDirectoryReader, int bufferSize) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader);
        int conuter =0;
        BufferedReader br;
        logger.info("Loading file {} to memory",arquivo.getName());
        try {
            br = new BufferedReader(new FileReader(arquivo), bufferSize);
            String line = null;
            try{
                while((line = br.readLine()) != null){
                    Application.addItemTolistManaged(line);                    
                    conuter++;
                }
                br.close();
                logger.info("File {} loaded on memory. Total items: {}.",arquivo.getName(), conuter);
            }catch(final Exception e){
                e.printStackTrace();
            }
        } catch (final IOException e1) {
            e1.printStackTrace();
        }
    }

}