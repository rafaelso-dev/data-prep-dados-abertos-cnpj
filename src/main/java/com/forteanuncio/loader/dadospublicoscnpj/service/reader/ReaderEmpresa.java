package com.forteanuncio.loader.dadospublicoscnpj.service.reader;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.forteanuncio.loader.dadospublicoscnpj.service.generator.GeneratorFileDelimited;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader {

    @Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader;

    @Value("${batchSize}")
    private int batchSize;

    @Autowired
    GeneratorFileDelimited generatorFile;

    static Map<String, List<String>> listaObjetosByHeader;

    public static boolean controle = false;

    private static final String GENERIC_FILE = "GenericFile";
    
    
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
                            new ReaderEmpresaExecutor(pathDirectoryReader, batchSize, generatorFile),
                            arquivo.getName());
                    t.start();
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void generateFile(GeneratorFileDelimited generatorFile, String key) {
        controle = true;
        try {
            if(key != null){
                generatorFile.addFile(new ArrayList<String>(listaObjetosByHeader.get(key)),new String(key), GENERIC_FILE);
                if(listaObjetosByHeader.get(key) != null){listaObjetosByHeader.get(key).clear();}
                listaObjetosByHeader.remove(key);
            }else{
                Set<String> lista = listaObjetosByHeader.keySet();
                List<String> list = new ArrayList<>();
                lista.forEach(item -> list.add(item));

                for(String keyIter:list){
                    generatorFile.addFile(new ArrayList<String>(listaObjetosByHeader.get(keyIter)),new String(keyIter), GENERIC_FILE);
                    listaObjetosByHeader.get(keyIter).clear();
                    listaObjetosByHeader.remove(keyIter);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        controle=false;
    }

    public GeneratorFileDelimited getGeneratorFile() {
        return generatorFile;
    }

    public void setGeneratorFile(GeneratorFileDelimited generatorFile) {
        this.generatorFile = generatorFile;
    }

}