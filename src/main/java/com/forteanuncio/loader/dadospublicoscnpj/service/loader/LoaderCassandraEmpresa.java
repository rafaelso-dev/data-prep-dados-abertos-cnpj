package com.forteanuncio.loader.dadospublicoscnpj.service.loader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class LoaderCassandraEmpresa implements Loader, Runnable {

    @Value("${pasta.escrita.empresas}")
    private String pathDirectory;

    @Value("${maxThreadSetSizeLoader}")
    private int maxThreadSetSizeLoader;

    @Value("${maxTimeWaitToLoader}")
    private long tempoMaximoEsperaParaCarregar;

    private boolean tempoMaximoEsperaParaCarregarExcedido;

    public static Map<String, Boolean> arquivosExecutando = new HashMap<String, Boolean>();
    public static int quantidadeThreadsExecutando = 0;

    @Override
    public void writerData() throws IOException, InterruptedException {
        File diretorio = new File(pathDirectory);
        if (!diretorio.exists() || !diretorio.isDirectory()) {
            throw new IOException("O destino especificado na propriedade 'pasta.escrita.empresas' não é um diretório");
        }
        percorrerDiretorio(diretorio.listFiles(), diretorio);
    }
    
    private void percorrerDiretorio(File[] arquivos, File diretorio) {
        maxThreadSetSizeLoader--;
        System.out.println("QUANTIDADE DE THREADS = "+maxThreadSetSizeLoader);
        try{
            Date primeiroMomento = new Date();
            Date segundoMomento;
            while(!tempoMaximoEsperaParaCarregarExcedido){
                segundoMomento = new Date();
                long diferencaSegundos = (segundoMomento.getTime() - primeiroMomento.getTime())/1000;
                if(diferencaSegundos > tempoMaximoEsperaParaCarregar){
                    tempoMaximoEsperaParaCarregarExcedido=true;
                }else{
                    arquivos = diretorio.listFiles();
                    for (File arquivo : arquivos) {
                        if(arquivosExecutando.get(arquivo.getName()) == null){
                            arquivosExecutando.put(arquivo.getName(),true);
                            Thread t = new Thread(new ThreadGroup("LoaderCsv"), this, arquivo.getName());
                            while(quantidadeThreadsExecutando > maxThreadSetSizeLoader){
                                Thread.sleep(200);
                            }
                            t.start();
                            quantidadeThreadsExecutando++;
                        }else if(!arquivosExecutando.get(arquivo.getName())){
                            arquivosExecutando.remove(arquivo.getName());
                        }
                        primeiroMomento = new Date();
                    }
                }
            }
        }catch(InterruptedException e){
            e.printStackTrace();
        }
    }

    @Override
    public void run() {
        try {
            File arquivo = new File(pathDirectory + Thread.currentThread().getName());
            ProcessBuilder builder = new ProcessBuilder();
            BufferedReader br = new BufferedReader(new FileReader(arquivo));
            String header = br.readLine();
            String command = "docker exec -i cassandra /opt/dse/bin/cqlsh -e \"copy dadosabertos.empresa("+header+") from '/config/"+arquivo.getName() + "' with delimiter='{' and header=true\"";
            br.close();
            builder.command("sh", "-c", command);
            builder.directory(new File(pathDirectory));
            Process process = builder.start();
            int exitVal = process.waitFor();
            System.out.println("Thread : "+ arquivo.getName());
            new BufferedReader(new InputStreamReader(process.getInputStream())).lines().forEach(System.out::println);
            new BufferedReader(new InputStreamReader(process.getErrorStream())).lines().forEach(System.err::println);
            if (exitVal == 0) {
                System.out.println(String.format("Arquivo %s importado com sucesso!", arquivo.getName()));
                System.out.println(String.format("Apagando arquivo principal %s", arquivo.getName()));
                if(arquivo.delete()){
                    System.out.println(String.format("Arquivo principal %s apagado", arquivo.getName()));
                    System.out.println(String.format("Removendo Flag de controle do mapa %s.", arquivo.getName()));
                    LoaderCassandraEmpresa.arquivosExecutando.put(arquivo.getName(), false);
                    quantidadeThreadsExecutando--;
                }else{
                    System.err.println(String.format("Arquivo principal %s não foi apagado", arquivo.getName()));
                }
            } else {
                throw new IOException("Aconteceu um erro na execução do arquivo:"+arquivo.getCanonicalPath()+"\n");
            }    
        } catch (IOException | InterruptedException e) {
            e.printStackTrace();
        }
    }

}