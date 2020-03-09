package com.forteanuncio.prep.dadospublicoscnpj.models;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CNAESecundaria implements Serializable{

    private static final long serialVersionUID = 2361902976097655601L;

    private Long cnpj;

    private Integer cnaeSecundaria;

}