package com.forteanuncio.prep.dadospublicoscnpj.generator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.mapper.MapperEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.GeneratorFileDelimitedEmpresaExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class GeneratorFileDelimitedEmpresa {

    @Value("${pasta.escrita.empresas}")
    private String pathDirectoryWriter;
    private ThreadPoolExecutor executor;
    
    private static Logger logger = LoggerFactory.getLogger(GeneratorFileDelimitedEmpresa.class);
    private static GeneratorFileDelimitedEmpresa generator;
    private static boolean executedThreadsGeneratorFiles = false;

    private GeneratorFileDelimitedEmpresa(){}

    public static int qtdRows = 0;

    public static GeneratorFileDelimitedEmpresa getInstance(){
        if(generator == null){
            generator = new GeneratorFileDelimitedEmpresa();
            generator.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(20);
        }
        return generator;
    }

    public void sendSinalToExecuteThreads(){
        if(!executedThreadsGeneratorFiles){
            executedThreadsGeneratorFiles = !executedThreadsGeneratorFiles;
            generator.generateFiles();
        }
    }

    

    public void generateFiles() {
        try{
            logger.info("Iniciando Gerador de arquivos"); 
            Iterator<String> iter = new HashMap<String, List<String>>(MapperEmpresa.mapObjetosByHeader).keySet().iterator();
            while(iter.hasNext()){
                String headerArquivo = iter.next();
                List<String> conteudoArquivo = MapperEmpresa.mapObjetosByHeader.get(headerArquivo);
                generator.executor.execute(new GeneratorFileDelimitedEmpresaExecutor(pathDirectoryWriter,headerArquivo,conteudoArquivo));
                Thread.sleep(100);
            }
            while(executor.getActiveCount() > 0){
                Thread.sleep(5);
            }
            executor.shutdown();
            logger.info("Finalizando Gerador de arquivos"); 
            logger.info("Total de linhas processadas até agora {}", qtdRows);
        }catch(Exception e){
            logger.error(e.getMessage());
            e.printStackTrace();
        }
    }

    public synchronized static void addLine(){
        qtdRows++;
    }
    
}