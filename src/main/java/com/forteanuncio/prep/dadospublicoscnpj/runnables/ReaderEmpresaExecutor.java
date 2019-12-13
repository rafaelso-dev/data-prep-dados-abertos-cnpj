package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.forteanuncio.prep.dadospublicoscnpj.model.Empresa;
import com.forteanuncio.prep.dadospublicoscnpj.service.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.service.reader.ReaderEmpresa;

import static com.forteanuncio.prep.dadospublicoscnpj.utils.ParseValueToTypeField.convertToObject;

public class ReaderEmpresaExecutor implements Runnable {

    private String pathDirectoryReader;

    public ReaderEmpresaExecutor(String pathDirectoryReader) {
        this.pathDirectoryReader = pathDirectoryReader;
    }

    @Override
    public void run() {
        
        File arquivo = new File(pathDirectoryReader + Thread.currentThread().getName());
        
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(arquivo));
            Empresa empresa = null;
            String line = null;
            try{
                while((line = br.readLine()) != null){
                    empresa = (Empresa) convertToObject(line, Empresa.class);
                    String genericHeader = GeneratorFileDelimitedEmpresa.generateHeaderByObject(empresa);
                    String lineFormated = GeneratorFileDelimitedEmpresa.convertToCsvByObject(empresa);
                    if(ReaderEmpresa.listaObjetosByHeader.get(genericHeader) == null){
                        List<String> lista = new ArrayList<String>();
                        lista.add(lineFormated);
                        ReaderEmpresa.listaObjetosByHeader.put(genericHeader,lista);
                    }else{
                        ReaderEmpresa.listaObjetosByHeader.get(genericHeader).add(lineFormated);
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

}