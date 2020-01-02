package com.forteanuncio.prep.dadospublicoscnpj.reader.impl;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.mapper.MapperEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.reader.Reader;
import com.forteanuncio.prep.dadospublicoscnpj.runnables.ReaderEmpresaExecutor;

import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader {

    //@Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader = "/media/rafael/dados/UFs/";

    public static Map<String, List<String>> mapObjetosByHeader = new HashMap<String, List<String>>();
    public static List<String> listLines = new ArrayList<String>();
    
    @Override
    public void reader() {
        File path = new File(pathDirectoryReader);
        try {

            if (!path.exists() || !path.isDirectory()) {
                throw new IOException("O destino especificado na propriedade 'pasta.leitura.empresas' não é um diretório");
            }
            File[] arquivos = path.listFiles();
            int bufferSize = 10485760/arquivos.length;
            for (File arquivo : arquivos) {
                if (arquivo.exists() && arquivo.canRead()) {
                    Thread t = new Thread(new ThreadGroup("ReaderEstadosCSV"), new ReaderEmpresaExecutor(pathDirectoryReader,bufferSize), arquivo.getName());
                    t.start();
                }
            }
            new MapperEmpresa();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    

}