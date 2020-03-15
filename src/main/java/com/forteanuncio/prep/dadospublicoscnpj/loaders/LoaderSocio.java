package com.forteanuncio.prep.dadospublicoscnpj.loaders;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.jmxloader.JmxBulkLoader;
import com.forteanuncio.prep.dadospublicoscnpj.writers.WriterSocio;

public class LoaderSocio implements Runnable{

    public void start() throws Exception {
        
        System.out.println("Loading Properties");
		Map<String, String>  properties = loadProperties();
		System.out.println("Properties Loaded");

        WriterSocio writerSocio = new WriterSocio(properties);
        String directory = "";
        if(isNotNullAndIsNotEmpty(properties.get("pasta.container.leitura"))){
            directory = properties.get("pasta.container.leitura");
        }else{
            throw new Exception("Diretório do container não foi mapeado");
        }

        JmxBulkLoader jmxLoader = new JmxBulkLoader("172.17.0.2", 7199);
        loadSocioByCnpj(jmxLoader, writerSocio, directory);
        jmxLoader.close();
        
    }

    public void loadSocioByCnpj(JmxBulkLoader jmxLoader, WriterSocio writerSocio, String tableName) throws Exception {
        writerSocio.socioByCnpj();
        tableName += writerSocio.getTableName();
        loadAndPurge(jmxLoader, tableName);
    }

    public void loadAndPurge(JmxBulkLoader jmxLoader, String tableName){
        jmxLoader.bulkLoad(tableName);
        jmxLoader.purgeDirectory(tableName);
    }

    @Override
    public void run(){
        try{
            start();
        }catch (Exception e){
            System.out.println(e.getMessage());
        }
    }

}