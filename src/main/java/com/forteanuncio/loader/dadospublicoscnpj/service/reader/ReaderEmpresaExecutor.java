package com.forteanuncio.loader.dadospublicoscnpj.service.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;
import com.forteanuncio.loader.dadospublicoscnpj.service.generator.GeneratorFileDelimited;

import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.convertToObject;

public class ReaderEmpresaExecutor implements Runnable {

    private String pathDirectoryReader;

    private int batchSize;

    GeneratorFileDelimited generatorFile;

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader + Thread.currentThread().getName());
        ReaderEmpresa.listaObjetosByHeader = new HashMap<String, List<String>>();
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(arquivo));
            Empresa empresa = null;
            String line = null;
            try{
                
                while((line = br.readLine()) != null){
                    if(!ReaderEmpresa.controle){
                        br.mark(0);
                        empresa = (Empresa) convertToObject(line, Empresa.class);
                        String genericHeader = generatorFile.generateHeaderByObject(empresa);
                        String lineFormated = generatorFile.convertToCsvByObject(empresa);
                        if(ReaderEmpresa.listaObjetosByHeader.get(genericHeader) == null){
                            List<String> lista = new ArrayList<String>();
                            lista.add(lineFormated);
                            ReaderEmpresa.listaObjetosByHeader.put(genericHeader,lista);
                        }else{
                            if(ReaderEmpresa.listaObjetosByHeader.get(genericHeader).size() == batchSize){
                                ReaderEmpresa.generateFile(generatorFile, genericHeader);
                            }else{
                                ReaderEmpresa.listaObjetosByHeader.get(genericHeader).add(lineFormated);
                            }
                        }
                    }else{
                        try{
                           br.reset();
                        }catch(Exception e){}
                        Thread.sleep(1000l);
                    }
                }
                br.close();
            }catch(Exception e){
                e.printStackTrace();
            }
            
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public ReaderEmpresaExecutor(String pathDirectoryReader, int batchSize, GeneratorFileDelimited generatorFile) {
        this.pathDirectoryReader = pathDirectoryReader;
        this.batchSize = batchSize;
        this.generatorFile = generatorFile;
    }

    
    
}