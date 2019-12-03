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
                int cont =0;
                while((line = br.readLine()) != null){
                    cont++;
                    empresa = (Empresa) convertToObject(line, Empresa.class);
                    String genericHeader = generatorFile.generateHeaderByObject(empresa);
                    String lineFormated = generatorFile.convertToCsvByObject(empresa);
                    if(listaObjetosByHeader.get(genericHeader) == null){
                        List<String> lista = new ArrayList<String>();
                        lista.add(lineFormated);
                        listaObjetosByHeader.put(genericHeader,lista);
                    }else{
                        if(listaObjetosByHeader.get(genericHeader).size() == batchSize){
                            generatorFile.addFile(listaObjetosByHeader.get(genericHeader),genericHeader);
                            listaObjetosByHeader.get(genericHeader).clear();
                            listaObjetosByHeader.remove(genericHeader);
                        }else{
                            listaObjetosByHeader.get(genericHeader).add(lineFormated);
                        }
                    }
                    if(cont%5000 == 0){
                        System.out.println("linhas percorridas "+cont);
                    }
                }
                br.close();
                
                Set<String> listaGenericHeaders = listaObjetosByHeader.keySet();
                for(String key : listaGenericHeaders){
                    generatorFile.addFile(listaObjetosByHeader.get(key),key);
                    listaObjetosByHeader.get(key).clear();
                    Thread.sleep(100);
                }

            }catch(Exception e){
                e.printStackTrace();
            }
            
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    
}