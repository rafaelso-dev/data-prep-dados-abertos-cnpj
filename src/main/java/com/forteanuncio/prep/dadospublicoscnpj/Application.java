package com.forteanuncio.prep.dadospublicoscnpj;

import java.io.IOException;
import java.util.Date;

import com.forteanuncio.prep.dadospublicoscnpj.generator.GeneratorFileDelimitedEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.reader.Reader;
import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;

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
		
		reader.reader();

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
