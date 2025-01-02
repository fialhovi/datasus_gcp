CREATE OR REPLACE PROCEDURE `datasus-prod.trusted.tb_sih_rd`()
BEGIN

-- Stored Procedure defining the columns to be used in the project and the data type of each one.
  CREATE OR REPLACE TABLE trusted.tb_sih_rd AS (
    SELECT
      UF_ZI                                                                   AS municipality_code,
      SAFE_CAST(CONCAT(ANO_CMPT, "-", LPAD(MES_CMPT, 2, "0"), "-01") AS DATE) AS date,
      ANO_CMPT                                                                AS year,
      MES_CMPT                                                                AS month,
      ESPEC                                                                   AS specialty_bed,
      N_AIH                                                                   AS aih,
      IDENT                                                                   AS type_aih,
      MUNIC_RES                                                               AS patient_municipality,
      SEXO                                                                    AS gender,
      MARCA_UTI                                                               AS type_uti,
      CAST(QT_DIARIAS AS INT64)                                               AS days_hospitalized,
      PROC_REA                                                                AS procedure,
      CAST(VAL_TOT AS FLOAT64)                                                AS total_value,
      MORTE                                                                   AS death,
      GESTRISCO                                                               AS risk_pregnant,
      RACA_COR                                                                AS race_color,
      SAFE_CAST(PARSE_DATE('%Y%m%d', NASC) AS DATE)                           AS birth_date,
      DATE_DIFF(CURRENT_DATE("America/Sao_Paulo"),
                SAFE_CAST(PARSE_DATE('%Y%m%d', NASC) AS DATE),
                YEAR)                                                         AS age

    FROM
      `datasus-prod.raw.tb_sih_rd`
  );

END;