package com.forteanuncio.prep.dadospublicoscnpj.models;

import java.io.Serializable;

import com.datastax.driver.core.LocalDate;

import lombok.AllArgsConstructor;
import lombok.Data;

@Data
@AllArgsConstructor
public class Socio implements Serializable{
    
    private static final long serialVersionUID = 911250549822685610L;

    private Long cnpj;

    private Long cpfCnpjSocio;

    private Integer identificadorSocio;

    private String nomeSocio;

    private String codigoQualificacaoSocio;

    private LocalDate dataEntradaSociedade;

    private String codigoPais;

    private String nomePaisSocio;

    private Long cpfRepresentanteLegal;

    private String nomeRepresentanteLegal;

    private String codigoQualificacaoRepresentanteLegal;

}