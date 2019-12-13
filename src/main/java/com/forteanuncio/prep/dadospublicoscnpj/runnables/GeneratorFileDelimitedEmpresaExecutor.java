package com.forteanuncio.prep.dadospublicoscnpj.runnables;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import org.apache.logging.log4j.util.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeneratorFileDelimitedEmpresaExecutor implements Runnable{

    Logger logger = LoggerFactory.getLogger(GeneratorFileDelimitedEmpresaExecutor.class);
    private String pathDirectoryWriter;
    private String genericHeader;
    private List<String> conteudoArquivo;
    
    public GeneratorFileDelimitedEmpresaExecutor(String pathDirectoryWriter, String genericHeader, List<String> conteudoArquivo){
        this.pathDirectoryWriter = pathDirectoryWriter;
        this.genericHeader = genericHeader;
        this.conteudoArquivo = conteudoArquivo;
    }

    @Override
    public void run() {
        try {
            String fileName = Thread.currentThread().getName()+"."+UUID.randomUUID().toString();
            File f = new File(pathDirectoryWriter);
            if (!f.exists()) {
                f.mkdirs();
            }
            if(!f.canWrite()){
                logger.error("Não é possível escrever arquivos no diretorio "+f.getCanonicalPath());
                throw new IOException("Não é possível escrever arquivos no diretorio "+f.getCanonicalPath());
            }
            
            f = new File(pathDirectoryWriter+fileName);
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            
            bw.append(genericHeader).append(Strings.LINE_SEPARATOR);
            for(int i=0; i < conteudoArquivo.size(); i++){
                bw.append(conteudoArquivo.get(i)).append(Strings.LINE_SEPARATOR);
                conteudoArquivo.remove(i);
                i--;
            }
            bw.close();
        } catch (IOException ex) {
            ex.printStackTrace();
         
        }
    }

}