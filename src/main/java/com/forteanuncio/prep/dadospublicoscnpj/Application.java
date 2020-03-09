package com.forteanuncio.prep.dadospublicoscnpj;

import com.forteanuncio.prep.dadospublicoscnpj.writers.WriterEmpresa;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Application {

	private static final Logger logger = LoggerFactory.getLogger(Application.class);
	
	public static void main(String[] args) {
		logger.info("Initializing Application");

		try {
			WriterEmpresa writer = new WriterEmpresa();
			writer.empresaByCnaeCnpjDataHoraInsercao();
			writer.empresaByCnaeRazaoSocialCnpjDataHoraInsercao();
			writer.empresaByUfCnaeCnpjDataHoraInsercao();
			writer.empresaByNomeFantasiaCnpjDataHoraInsercao();
			writer.empresaByUfMunicipioCnpjDataHoraInsercao();

		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		logger.info("Finishing Application");
	}
	
}
