package com.forteanuncio.prep.dadospublicoscnpj.models;

import java.io.Serializable;

import com.datastax.driver.core.LocalDate;

public class Socio implements Serializable{
    
    private static final long serialVersionUID = 911250549822685610L;

    private Long cnpj;
    
    private Integer identificadorSocio;
    // 1 - Pessoa Juridica
    // 2 - Pessoa Fisica
    // 3 - Estrangeiro
    
    private String nomeSocio;

    private Long cpfCnpjSocio;

    private String codigoQualificacaoSocio;

    private Float percentualCapitalSocial;

    private LocalDate dataEntradaSociedade;

    private Integer codigoPais;

    private String nomePaisSocio;

    private Long cpfRepresentanteLegal;

    private String nomeRepresentanteLegal;

    private String codigoQualificacaoRepresentanteLegal;

}