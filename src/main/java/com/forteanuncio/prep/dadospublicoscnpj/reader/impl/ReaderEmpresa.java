package com.forteanuncio.prep.dadospublicoscnpj.reader.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.mapper.MapperEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.reader.Reader;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.ReaderEmpresaExecutor;

import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader {

    //@Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader = "/media/rafael/dados/UFs/";

    public static List<String> listLines = new ArrayList<String>();
    
    @Override
    public void reader() {
        File path = new File(pathDirectoryReader);
        try {

            if (!path.exists() || !path.isDirectory()) {
                throw new IOException("O destino especificado na propriedade 'pasta.leitura.empresas' não é um diretório");
            }
            File[] arquivos = path.listFiles();
            int bufferSize = 104857600/arquivos.length;
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(arquivos.length);
            for (File arquivo : arquivos) {
                if (arquivo.exists() && arquivo.canRead()) {
                    executor.execute(new ReaderEmpresaExecutor(pathDirectoryReader+arquivo.getName(),bufferSize));
                }
            }
            while(executor.getActiveCount() > 0){
                Thread.sleep(5);
            }
            executor.shutdown();
            new MapperEmpresa();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

}