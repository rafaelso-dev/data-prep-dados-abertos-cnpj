package com.forteanuncio.prep.dadospublicoscnpj;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import com.forteanuncio.prep.dadospublicoscnpj.loaders.LoaderCnaeSecundaria;
import com.forteanuncio.prep.dadospublicoscnpj.loaders.LoaderEmpresa;
import com.forteanuncio.prep.dadospublicoscnpj.loaders.LoaderSocio;
import com.forteanuncio.prep.dadospublicoscnpj.utils.Timer;

public class Application {

	public static void main(String[] args) {
		System.out.println("Initializing Application");

		try {
			Timer timer = new Timer();
			timer.start();

			ExecutorService executores = Executors.newFixedThreadPool(3);
			executores.execute(new LoaderEmpresa());
			executores.execute(new LoaderCnaeSecundaria());
			executores.execute(new LoaderSocio());
			executores.wait();
			timer.end();
			System.out.println("Tempo Decorrido" + timer.getTimeTakenSeconds());
			
		} catch (Exception e) {
			System.out.println(e.getMessage());
		}
		System.out.println("Finishing Application");
	}
	
	

}
