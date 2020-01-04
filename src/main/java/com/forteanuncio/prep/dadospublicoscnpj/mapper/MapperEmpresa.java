package com.forteanuncio.prep.dadospublicoscnpj.mapper;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

import com.forteanuncio.prep.dadospublicoscnpj.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.MapperEmpresaExecutor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MapperEmpresa {

    public static Map<String, List<String>> mapObjetosByHeader = new HashMap<String, List<String>>();
    
    private static final Logger logger = LoggerFactory.getLogger(MapperEmpresa.class);

    public MapperEmpresa() {
        startThreads();
    }

    public void startThreads() {
        try {
            ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(10);
            //logger.info("Criando ThreadPool de mapper Empresa com tamanho de 1");
            
            logger.info("Iniciando executor de mapeamento MapperEmpresaExecutor");
            while(ReaderEmpresa.listLines.size() > 0) {
                ////new MapperEmpresaExecutor(removeItemLista(ReaderEmpresa.listLines, 0)).run();
                executor.execute(new MapperEmpresaExecutor(removeItemLista(ReaderEmpresa.listLines, 0)));
            }

            logger.info("Finalizando executor de mapeamento MapperEmpresaExecutor");
            
            while(executor.getActiveCount() > 0){
                Thread.sleep(5);
            }
            executor.shutdown();
            GeneratorFileDelimitedEmpresa.getInstance().sendSinalToExecuteThreads();
        } catch (Exception e) {
            e.printStackTrace();
        }
        
    }
    private synchronized String removeItemLista(List<String> lista, int index){
        return lista.remove(index);
    }
    public static synchronized void adiciona(String key, List<String> lista){
        MapperEmpresa.mapObjetosByHeader.put(key, lista);
    }
    public static synchronized void adiciona(String key, String item){
        MapperEmpresa.mapObjetosByHeader.get(key).add(item);
    }

}