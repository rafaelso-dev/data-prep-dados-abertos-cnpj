package com.forteanuncio.loader.dadospublicoscnpj;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Set;

import com.forteanuncio.loader.dadospublicoscnpj.service.generator.GeneratorFileDelimited;
import com.forteanuncio.loader.dadospublicoscnpj.service.loader.Loader;
import com.forteanuncio.loader.dadospublicoscnpj.service.reader.Reader;
import com.forteanuncio.loader.dadospublicoscnpj.service.reader.ReaderEmpresa;

import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.data.cassandra.config.CassandraClusterFactoryBean;

@SpringBootApplication
@ComponentScan(basePackages = "com.forteanuncio.loader.dadospublicoscnpj")
public class Application {

	static Reader reader;
	static Loader loader;
	static CassandraClusterFactoryBean cassandraConnection;
	static GeneratorFileDelimited generatorFile;

	public static void main(String[] args) throws IOException, InterruptedException {

		ApplicationContext ac = SpringApplication.run(Application.class, args);
		reader = ac.getBean(ReaderEmpresa.class);
		loader = ac.getBean(Loader.class);
		generatorFile = ac.getBean(GeneratorFileDelimited.class);

		System.out.println("Obtendo Bean do Cassandra");
		cassandraConnection = ac.getBean(CassandraClusterFactoryBean.class);

		System.out.println("Encerrando conexão com o cassandra - tabelas já criadas");
		cassandraConnection.destroy();

		System.out.println("Inicio do processamento " + new Date());

		reader.reader();

		// loader.writerData();

		otherWait("ReaderEstadosCSV");

		SpringApplication.exit(ac, new ExitCodeSuccessfully());
	}

	static class ExitCodeSuccessfully implements ExitCodeGenerator{
		@Override
		public int getExitCode() {
			System.out.println("Fim do processamento "+new Date());
			return 0;
		}
	}

	public static boolean otherWait(String threadGroupName){
		boolean saida = true;
		Set<Thread> set = Thread.getAllStackTraces().keySet();
		List<Thread> lista = new ArrayList<>();
		set.forEach(t -> lista.add(t));
		for(int i=0; i < lista.size(); i++){
			if (lista.get(i).getThreadGroup().getName().equals(threadGroupName)) {
				try {
					Thread.sleep(5000l);
					saida = false;
					break;
				} catch (InterruptedException e) {
					e.printStackTrace();
					saida = false;
				}
			}
		}
		if(saida){ 
			ReaderEmpresa.generateFile(generatorFile,null);
		}else{
			saida = otherWait(threadGroupName);
		}
		return saida;
	}

}
