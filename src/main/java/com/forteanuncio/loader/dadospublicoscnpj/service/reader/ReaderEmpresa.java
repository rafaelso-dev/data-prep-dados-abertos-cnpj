package com.forteanuncio.loader.dadospublicoscnpj.service.reader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;
import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.convertToObject;
import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.convertToCsv;
import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.generateHeaderOfClass;

import org.apache.logging.log4j.util.Strings;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class ReaderEmpresa implements Reader, Runnable {

    @Value("${pasta.leitura.empresas}")
    private String pathDirectoryReader;

    @Value("${pasta.escrita.empresas}")
    private String pathDirectoryWriter;
    
    private static final String HEADERFILE = generateHeaderOfClass(Empresa.class);

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
                    Thread t = new Thread(this, arquivo.getName());
                    t.start();
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        int contLines = 0;
        int contFiles = 0;
        File arquivo = new File(pathDirectoryReader + Thread.currentThread().getName());
        StringBuilder sb = new StringBuilder();
        
        BufferedReader br;
        try {
            br = new BufferedReader(new FileReader(arquivo));
            Empresa empresa = null;
            String line = null;
            try{
                
                while((line = br.readLine()) != null){
                    empresa = (Empresa) convertToObject(line, Empresa.class);
                    String lineFormated = convertToCsv(empresa);
                    if(lineFormated.split("\\{").length > 39){
                        System.out.println(lineFormated+", arquivo "+ contFiles+" , linha "+ contLines);
                    }
                    sb.append(lineFormated).append(Strings.LINE_SEPARATOR);
                    contLines++;
                    
                    if(contLines == 5000){
                        contFiles++;
                        adicionaArquivo(arquivo, contFiles, sb);
                        contLines = 0;
                        sb = new StringBuilder();
                    }
                }
                // System.out.println(contLines);
                if(contLines > 0){
                    adicionaArquivo(arquivo, ++contFiles, sb);
                }
            }catch(Exception e){
                e.printStackTrace();
            }
            br.close();
        } catch (IOException e1) {
            e1.printStackTrace();
        }
    }

    public void adicionaArquivo(File arquivo, int contFiles, StringBuilder sb){
        try{
            File f = new File(pathDirectoryWriter);
            if(!f.exists()){
                f.mkdirs();
            }
            f = new File(f.getCanonicalPath()+"/"+arquivo.getName()+"."+contFiles);
            BufferedWriter bw = new BufferedWriter(new FileWriter(f));
            bw.append(HEADERFILE).append(Strings.LINE_SEPARATOR).append(sb.toString());
            bw.close();
        }catch(IOException ex){
            ex.printStackTrace();
        }
    }

}