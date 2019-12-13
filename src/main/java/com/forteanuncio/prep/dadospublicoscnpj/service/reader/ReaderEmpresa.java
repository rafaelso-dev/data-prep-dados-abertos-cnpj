package com.forteanuncio.prep.dadospublicoscnpj.service.reader;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.forteanuncio.prep.dadospublicoscnpj.runnables.ReaderEmpresaExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader {

    @Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader;

    public static Map<String, List<String>> listaObjetosByHeader = new HashMap<String, List<String>>();

    public static Object generatorFile;
    
    @Override
    public void reader() {
        File path = new File(pathDirectoryReader);
        try {

            if (!path.exists() || !path.isDirectory()) {
                throw new IOException("O destino especificado na propriedade 'pasta.leitura.empresas' não é um diretório");
            }
            File[] arquivos = path.listFiles();
            for (File arquivo : arquivos) {
                if (arquivo.exists() && arquivo.canRead()) {
                    Thread t = new Thread(new ThreadGroup("ReaderEstadosCSV"),
                            new ReaderEmpresaExecutor(pathDirectoryReader),
                            arquivo.getName());
                    t.start();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }


}