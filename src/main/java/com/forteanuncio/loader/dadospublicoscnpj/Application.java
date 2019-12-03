package com.forteanuncio.loader.dadospublicoscnpj;

import java.io.IOException;
import java.util.Date;

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
	public static void main(String[] args) throws IOException, InterruptedException {
		
		ApplicationContext ac = SpringApplication.run(Application.class, args);
		reader = ac.getBean(ReaderEmpresa.class);
		loader = ac.getBean(Loader.class);
		
		System.out.println("Obtendo Bean do Cassandra");
		cassandraConnection = ac.getBean(CassandraClusterFactoryBean.class);

		System.out.println("Encerrando conexão com o cassandra - tabelas já criadas");
		cassandraConnection.destroy();
		
		System.out.println("Inicio do processamento "+new Date());
		
		reader.reader();

		loader.writerData();
		
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
