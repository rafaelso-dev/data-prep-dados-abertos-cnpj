package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import org.slf4j.LoggerFactory;
import org.slf4j.Logger;

import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;

public class ReaderEmpresaExecutor implements Runnable {

    private String pathDirectoryReader;
    private Integer bufferSize;

    private static final Logger logger = LoggerFactory.getLogger(ReaderEmpresaExecutor.class);

    public ReaderEmpresaExecutor(String pathDirectoryReader, int bufferSize) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader);
        
        BufferedReader br;
        logger.info("Subindo arquivo {} para a memória",arquivo.getName());
        try {
            br = new BufferedReader(new FileReader(arquivo), bufferSize);
            String line = null;
            try{
                while((line = br.readLine()) != null){
                    ReaderEmpresa.listLines.add(line);
                }
                br.close();
                logger.info("Arquivo {} foi carregado na memória. Total de itens:  {}.",arquivo.getName(),ReaderEmpresa.listLines.size());
            }catch(final Exception e){
                e.printStackTrace();
            }
        } catch (final IOException e1) {
            e1.printStackTrace();
        }
    }

}