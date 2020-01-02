package com.forteanuncio.prep.dadospublicoscnpj;

import java.io.IOException;
import java.util.Date;

import com.forteanuncio.prep.dadospublicoscnpj.reader.impl.ReaderEmpresa;

import org.springframework.boot.ExitCodeGenerator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;


@SpringBootApplication
@ComponentScan(basePackages = "com.forteanuncio.prep.dadospublicoscnpj")
public class Application {

	public static void main(String[] args) throws IOException, InterruptedException {
		
		System.out.println("Inicio do processamento "+new Date());
		new ReaderEmpresa();
	}

	static class ExitCodeSuccessfully implements ExitCodeGenerator{
		@Override
		public int getExitCode() {
			System.out.println("Fim do processamento "+new Date());
			return 0;
		}
	}

}
