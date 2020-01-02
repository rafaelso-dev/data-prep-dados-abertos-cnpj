package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;

public class ReaderEmpresaExecutor implements Runnable {

    private String pathDirectoryReader;
    private Integer bufferSize;
    public ReaderEmpresaExecutor(String pathDirectoryReader, int bufferSize) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.bufferSize = bufferSize;
    }

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader + Thread.currentThread().getName());
        
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(arquivo), bufferSize);
            String line = null;
            try{
                while((line = br.readLine()) != null){
                    ReaderEmpresa.listLines.add(line);
                }
                br.close();
            }catch(final Exception e){
                e.printStackTrace();
            }
            
        } catch (final IOException e1) {
            e1.printStackTrace();
        }
    }

}