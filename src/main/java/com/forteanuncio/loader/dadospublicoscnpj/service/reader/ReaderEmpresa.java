package com.forteanuncio.loader.dadospublicoscnpj.service.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;
import com.forteanuncio.loader.dadospublicoscnpj.service.generator.GeneratorFileDelimited;

import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.convertToObject;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader, Runnable {

    @Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader;

    @Value("${batchSize}")
    private int batchSize;

    @Autowired
    GeneratorFileDelimited generatorFile;

    Map<String, List<String>> listaObjetosByHeader;

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
                    Thread t = new Thread(new ThreadGroup("ReaderEstadosCSV"),this, arquivo.getName());
                    t.start();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader + Thread.currentThread().getName());
        listaObjetosByHeader = new HashMap<String, List<String>>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(arquivo));
            Empresa empresa = null;
            String line = null;
            try{
                
                while((line = br.readLine()) != null){
                    empresa = (Empresa) convertToObject(line, Empresa.class);
                    String genericHeader = generatorFile.generateHeaderByObject(empresa);
                    String lineFormated = generatorFile.convertToCsvByObject(empresa);
                    List<String> lista;
                    
                    if(listaObjetosByHeader.get(genericHeader) == null){
                        lista = new ArrayList<String>();
                        lista.add(lineFormated);
                        listaObjetosByHeader.put(genericHeader,lista);
                    }else{
                        lista = listaObjetosByHeader.get(genericHeader);
                        if(lista.size() == batchSize){
                            generatorFile.addFile(lista,genericHeader);
                            listaObjetosByHeader.remove(genericHeader);
                            lista.clear();
                        }else{
                            listaObjetosByHeader.get(genericHeader).add(lineFormated);
                        }
                    }
                }

                Set<String> listaGenericHeaders = listaObjetosByHeader.keySet();
                for(String key : listaGenericHeaders){
                    generatorFile.addFile(listaObjetosByHeader.get(key),key);
                    listaObjetosByHeader.put(key,null);
                }

            }catch(Exception e){
                e.printStackTrace();
            }
            br.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    
}