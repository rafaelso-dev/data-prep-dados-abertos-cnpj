package com.forteanuncio.prep.dadospublicoscnpj.loaders;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.isNotNullAndIsNotEmpty;
import static com.forteanuncio.prep.dadospublicoscnpj.utils.Utils.loadProperties;

import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.jmxloader.JmxBulkLoader;
import com.forteanuncio.prep.dadospublicoscnpj.writers.WriterEmpresa;

public class LoaderEmpresa implements Runnable{

    public void start() throws Exception {
        
        System.out.println("Loading Properties");
		Map<String, String>  properties = loadProperties();
		System.out.println("Properties Loaded");

        WriterEmpresa writerEmpresa = new WriterEmpresa(properties);
        String directory = "";
        if(isNotNullAndIsNotEmpty(properties.get("pasta.container.leitura"))){
            directory = properties.get("pasta.container.leitura");
        }else{
            throw new Exception("Diretório do container não foi mapeado");
        }

        JmxBulkLoader jmxLoader = new JmxBulkLoader("172.17.0.2", 7199);
        loadEmpresaByCnaeCnpjDataHoraInsercao(jmxLoader, writerEmpresa, directory);
        loadEmpresaByCnaeRazaoSocialCnpjDataHoraInsercao(jmxLoader, writerEmpresa, directory);
        loadEmpresaByUfCnaeCnpjDataHoraInsercao(jmxLoader, writerEmpresa, directory);
        loadEmpresaByNomeFantasiaCnpjDataHoraInsercao(jmxLoader, writerEmpresa, directory);
        loadEmpresaByUfMunicipioCnpjDataHoraInsercao(jmxLoader, writerEmpresa, directory);
        jmxLoader.close();
        
    }

    public void loadEmpresaByCnaeCnpjDataHoraInsercao(JmxBulkLoader jmxLoader, WriterEmpresa writerEmpresa, String tableName) throws Exception {
        writerEmpresa.empresaByCnaeCnpjDataHoraInsercao();
        tableName += writerEmpresa.getTableName();
        loadAndPurge(jmxLoader, tableName);
    }

    public void loadEmpresaByCnaeRazaoSocialCnpjDataHoraInsercao(JmxBulkLoader jmxLoader, WriterEmpresa writerEmpresa, String tableName) throws Exception {
        writerEmpresa.empresaByCnaeRazaoSocialCnpjDataHoraInsercao();
        tableName += writerEmpresa.getTableName();
        loadAndPurge(jmxLoader, tableName);
    }

    public void loadEmpresaByUfCnaeCnpjDataHoraInsercao(JmxBulkLoader jmxLoader, WriterEmpresa writerEmpresa, String tableName) throws Exception {
        writerEmpresa.empresaByUfCnaeCnpjDataHoraInsercao();
        tableName += writerEmpresa.getTableName();
        loadAndPurge(jmxLoader, tableName);
    }
    
    public void loadEmpresaByNomeFantasiaCnpjDataHoraInsercao(JmxBulkLoader jmxLoader, WriterEmpresa writerEmpresa, String tableName) throws Exception {
        writerEmpresa.empresaByNomeFantasiaCnpjDataHoraInsercao();
        tableName += writerEmpresa.getTableName();
        loadAndPurge(jmxLoader, tableName);
    }

    public void loadEmpresaByUfMunicipioCnpjDataHoraInsercao(JmxBulkLoader jmxLoader, WriterEmpresa writerEmpresa, String tableName) throws Exception {
        writerEmpresa.empresaByUfMunicipioCnpjDataHoraInsercao();
        tableName += writerEmpresa.getTableName();
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