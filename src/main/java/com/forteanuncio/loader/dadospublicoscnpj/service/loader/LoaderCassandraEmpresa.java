package com.forteanuncio.loader.dadospublicoscnpj.service.loader;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.concurrent.Executors;

import com.forteanuncio.loader.dadospublicoscnpj.model.Empresa;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import static com.forteanuncio.loader.dadospublicoscnpj.utils.ParseValueToTypeField.generateHeaderOfClass;

@Component
public class LoaderCassandraEmpresa implements Loader, Runnable {

    private static String RUNNING = ".running";

    private static final String HEADERFILE = generateHeaderOfClass(Empresa.class);

    @Value("${pasta.escrita.empresas}")
    private String pathDirectory;

    @Override
    public void writerData() throws IOException, InterruptedException {

        File f = new File(pathDirectory);
        if (!f.exists() || !f.isDirectory()) {
            throw new IOException("O destino especificado na propriedade 'pasta.escrita.empresas' não é um diretório");
        }
        File[] arquivos = f.listFiles();

        while(arquivos.length > 0){
            for (File arquivo : arquivos) {

                if (!arquivo.getName().endsWith(RUNNING)) {
                    if(!new File(arquivo.getCanonicalPath()+RUNNING).exists()){
                        BufferedWriter bw = new BufferedWriter(new FileWriter(new File(arquivo.getCanonicalPath() + RUNNING)));
                        bw.write(RUNNING);
                        bw.close();
                        Thread t = new Thread(this, arquivo.getName());
                        while(Thread.activeCount() > 9){
                            Thread.sleep(500);
                        }
                        t.start();
                    }
                }
            }
            arquivos = f.listFiles();
        }
    }

    @Override
    public synchronized void run() {
        try {
            File arquivo = new File(pathDirectory + Thread.currentThread().getName());

            ProcessBuilder builder = new ProcessBuilder();
            String command = "docker exec -i cassandra /opt/dse/bin/cqlsh -e \"copy dadosabertos.empresa("+HEADERFILE+") from '/config/"+arquivo.getName() + "' with delimiter='{' and header=true\"";
            builder.command("sh", "-c", command);
            builder.directory(new File(pathDirectory));
            Process process = builder.start();
            int exitVal = process.waitFor();
            System.out.println("Thread : "+ arquivo.getName());
            new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach(System.out::println);
            new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach(System.out::println);
            if (exitVal == 0) {
                System.out.println(String.format("Arquivo %s importado com sucesso!", arquivo.getName()));
                System.out.println(String.format("Apagando arquivo principal %s", arquivo.getName()));
                if(arquivo.delete()){
                    System.out.println(String.format("Arquivo principal %s apagado", arquivo.getName()));
                    System.out.println(String.format("Apagando arquivo de controle %s%s",arquivo,RUNNING));
                    new File(arquivo.getCanonicalPath()+RUNNING).delete();
                    System.out.println(String.format("Arquivo de controle %s%s apagado",arquivo,RUNNING));
                }
            } else {
                throw new IOException("Aconteceu um erro na execução do arquivo:"+arquivo.getCanonicalPath()+"\n");
            }    
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}