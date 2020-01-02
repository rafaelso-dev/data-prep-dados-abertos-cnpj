package com.forteanuncio.prep.dadospublicoscnpj.mapper;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.MapperEmpresaExecutor;

public class MapperEmpresa {

    public MapperEmpresa() {
        startThreads();
    }

    public void startThreads() {
        try {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            System.out.println("Criou o ThreadPool do mapper Empresa, tamanho da lista : " + ReaderEmpresa.listLines.size());
            
            for (int i = 0; i < ReaderEmpresa.listLines.size(); i++) {
                executor.execute(new MapperEmpresaExecutor(new String(ReaderEmpresa.listLines.get(i))));
                // ReaderEmpresa.listLines.remove(i);
                // i--;
            }
            while (executor.getActiveCount() > 0) {
                Thread.sleep(10);
            }
            executor.shutdown();
            new GeneratorFileDelimitedEmpresa();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        
    }

}