package com.forteanuncio.prep.dadospublicoscnpj;

import java.io.IOException;
import java.util.Date;
import java.util.Set;

import com.forteanuncio.prep.dadospublicoscnpj.service.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.service.reader.Reader;
import com.forteanuncio.prep.dadospublicoscnpj.service.reader.ReaderEmpresa;

import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;

@SpringBootApplication
@ComponentScan(basePackages = "com.forteanuncio.prep.dadospublicoscnpj")
public class Application {

	static Reader reader;
	static GeneratorFileDelimitedEmpresa generatorFile;
	static CassandraClusterFactoryBean cassandraConnection;
	public static void main(String[] args) throws IOException, InterruptedException {
		
		ApplicationContext ac = SpringApplication.run(Application.class, args);
		reader = ac.getBean(ReaderEmpresa.class);
		generatorFile = ac.getBean(GeneratorFileDelimitedEmpresa.class);
		
		System.out.println("Obtendo Bean do Cassandra");
		cassandraConnection = ac.getBean(CassandraClusterFactoryBean.class);

		System.out.println("Encerrando conexão com o cassandra - tabelas já criadas");
		cassandraConnection.destroy();
		
		System.out.println("Inicio do processamento "+new Date());
		
		reader.reader();
		
		//espera um pouco para começar a ler os arquivos
		Thread.sleep(5000);

		Set<Thread> listaThreads = Thread.getAllStackTraces().keySet();
		boolean existeThreadsMinhas = true;
		while(existeThreadsMinhas){
			
			//Seta false na variavel, pois não sei se ainda existe Threads
			existeThreadsMinhas = false;

			//varre o array verificando se ainda existe Thread
			for(Thread thread : listaThreads){
				if(thread.getThreadGroup() != null && thread.getThreadGroup().getName().equals("ReaderEstadosCSV")){
					existeThreadsMinhas = true;
					break;
				}
			}
			if(existeThreadsMinhas){
				//refaz o mapa de THreads e seta na lista
				listaThreads = Thread.getAllStackTraces().keySet();
				//VOLTA PRO começo
				Thread.sleep(5000);
			}else{
				//chama o generator File para gerar os arquivos, pois o mapeamento acabou
				generatorFile.generateFiles();
			}
		}

		SpringApplication.exit(ac, new ExitCodeSuccessfully());
	}

	static class ExitCodeSuccessfully implements ExitCodeGenerator{
		@Override
		public int getExitCode() {
			System.out.println("Fim do processamento "+new Date());
			return 0;
		}
	}

}
