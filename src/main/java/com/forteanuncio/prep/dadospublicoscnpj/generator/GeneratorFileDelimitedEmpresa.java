package com.forteanuncio.prep.dadospublicoscnpj.generator;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.runnables.GeneratorFileDelimitedEmpresaExecutor;
import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;
import org.springframework.stereotype.Component;

@Component
public class GeneratorFileDelimitedEmpresa {

    private String pathDirectoryWriter = "/media/rafael/dados/volume_cassandra/";

    public GeneratorFileDelimitedEmpresa(){
        generateFiles();
    }

    public void generateFiles() {
        try{
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            Set<String> listaHeaders = ReaderEmpresa.mapObjetosByHeader.keySet();
            
            // need use a threadpool with 10 thread
            for(String headerArquivo : listaHeaders){
                List<String> conteudoArquivo = ReaderEmpresa.mapObjetosByHeader.get(headerArquivo);
                executor.execute(new GeneratorFileDelimitedEmpresaExecutor(pathDirectoryWriter,headerArquivo,conteudoArquivo));
            }
            while(executor.getActiveCount() > 0){
                Thread.sleep(10);
            }
            executor.shutdown();
        }catch(InterruptedException e){
            e.printStackTrace();
        }

    }

    

    
}