package com.forteanuncio.prep.dadospublicoscnpj.generator;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.mapper.MapperEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.GeneratorFileDelimitedEmpresaExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class GeneratorFileDelimitedEmpresa {

    private String pathDirectoryWriter = "/media/rafael/dados/volume_cassandra/";
    public static boolean executedThreadsGeneratorFiles = false;
    private static Logger logger = LoggerFactory.getLogger(GeneratorFileDelimitedEmpresa.class);

    
    private ThreadPoolExecutor executor;

    private static GeneratorFileDelimitedEmpresa eu;

    public static int qtdRows = 0;

    public static GeneratorFileDelimitedEmpresa getInstance(){
        if(eu == null){
            eu = new GeneratorFileDelimitedEmpresa();
            eu.executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
        }
        return eu;
    }

    public void sendSinalToExecuteThreads(){
        if(!executedThreadsGeneratorFiles){
            executedThreadsGeneratorFiles = !executedThreadsGeneratorFiles;
            eu.generateFiles();
        }
    }

    private GeneratorFileDelimitedEmpresa(){}

    public void generateFiles() {
        try{
            logger.info("Iniciando Gerador de arquivos"); 
            Iterator<String> iter = new HashMap<String, List<String>>(MapperEmpresa.mapObjetosByHeader).keySet().iterator();
            while(iter.hasNext()){
                String headerArquivo = iter.next();
                List<String> conteudoArquivo = MapperEmpresa.mapObjetosByHeader.get(headerArquivo);
                eu.executor.execute(new GeneratorFileDelimitedEmpresaExecutor(pathDirectoryWriter,headerArquivo,conteudoArquivo));
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