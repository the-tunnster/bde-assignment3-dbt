CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS WAREHOUSE;
CREATE SCHEMA IF NOT EXISTS DATAMART;

---------------------------
-- Raw
---------------------------

CREATE TABLE IF NOT EXISTS RAW.census_data_g01 (
	LGA_CODE VARCHAR(255) PRIMARY KEY ,
    Tot_P_M INT,
    Tot_P_F INT,
    Tot_P_P INT,
    Age_0_4_yr_M INT,
    Age_0_4_yr_F INT,
    Age_0_4_yr_P INT,
    Age_5_14_yr_M INT,
    Age_5_14_yr_F INT,
    Age_5_14_yr_P INT,
    Age_15_19_yr_M INT,
    Age_15_19_yr_F INT,
    Age_15_19_yr_P INT,
    Age_20_24_yr_M INT,
    Age_20_24_yr_F INT,
    Age_20_24_yr_P INT,
    Age_25_34_yr_M	INT,
    Age_25_34_yr_F	INT,
    Age_25_34_yr_P	INT,
    Age_35_44_yr_M	INT,
    Age_35_44_yr_F	INT,
    Age_35_44_yr_P	INT,
    Age_45_54_yr_M	INT,
    Age_45_54_yr_F	INT,
    Age_45_54_yr_P	INT,
    Age_55_64_yr_M	INT,
    Age_55_64_yr_F	INT,
    Age_55_64_yr_P	INT,
    Age_65_74_yr_M	INT,
    Age_65_74_yr_F	INT,
    Age_65_74_yr_P	INT,
    Age_75_84_yr_M	INT,
    Age_75_84_yr_F	INT,
    Age_75_84_yr_P	INT,
    Age_85ov_M	INT,
    Age_85ov_F	INT,
    Age_85ov_P	INT,
    Counted_Census_Night_home_M	INT,
    Counted_Census_Night_home_F	INT,
    Counted_Census_Night_home_P	INT,
    Count_Census_Nt_Ewhere_Aust_M	INT,
    Count_Census_Nt_Ewhere_Aust_F	INT,
    Count_Census_Nt_Ewhere_Aust_P	INT,
    Indigenous_psns_Aboriginal_M	INT,
    Indigenous_psns_Aboriginal_F	INT,
    Indigenous_psns_Aboriginal_P	INT,
    Indig_psns_Torres_Strait_Is_M	INT,
    Indig_psns_Torres_Strait_Is_F	INT,
    Indig_psns_Torres_Strait_Is_P	INT,
    Indig_Bth_Abor_Torres_St_Is_M	INT,
    Indig_Bth_Abor_Torres_St_Is_F	INT,
    Indig_Bth_Abor_Torres_St_Is_P	INT,
    Indigenous_P_Tot_M	INT,
    Indigenous_P_Tot_F	INT,
    Indigenous_P_Tot_P	INT,
    Birthplace_Australia_M	INT,
    Birthplace_Australia_F	INT,
    Birthplace_Australia_P	INT,
    Birthplace_Elsewhere_M	INT,
    Birthplace_Elsewhere_F	INT,
    Birthplace_Elsewhere_P	INT,
    Lang_spoken_home_Eng_only_M	INT,
    Lang_spoken_home_Eng_only_F	INT,
    Lang_spoken_home_Eng_only_P	INT,
    Lang_spoken_home_Oth_Lang_M	INT,
    Lang_spoken_home_Oth_Lang_F	INT,
    Lang_spoken_home_Oth_Lang_P	INT,
    Australian_citizen_M	INT,
    Australian_citizen_F	INT,
    Australian_citizen_P	INT,
    Age_psns_att_educ_inst_0_4_M	INT,
    Age_psns_att_educ_inst_0_4_F	INT,
    Age_psns_att_educ_inst_0_4_P	INT,
    Age_psns_att_educ_inst_5_14_M	INT,
    Age_psns_att_educ_inst_5_14_F	INT,
    Age_psns_att_educ_inst_5_14_P	INT,
    Age_psns_att_edu_inst_15_19_M	INT,
    Age_psns_att_edu_inst_15_19_F	INT,
    Age_psns_att_edu_inst_15_19_P	INT,
    Age_psns_att_edu_inst_20_24_M	INT,
    Age_psns_att_edu_inst_20_24_F	INT,
    Age_psns_att_edu_inst_20_24_P	INT,
    Age_psns_att_edu_inst_25_ov_M	INT,
    Age_psns_att_edu_inst_25_ov_F	INT,
    Age_psns_att_edu_inst_25_ov_P	INT,
    High_yr_schl_comp_Yr_12_eq_M	INT,
    High_yr_schl_comp_Yr_12_eq_F	INT,
    High_yr_schl_comp_Yr_12_eq_P	INT,
    High_yr_schl_comp_Yr_11_eq_M	INT,
    High_yr_schl_comp_Yr_11_eq_F	INT,
    High_yr_schl_comp_Yr_11_eq_P	INT,
    High_yr_schl_comp_Yr_10_eq_M	INT,
    High_yr_schl_comp_Yr_10_eq_F	INT,
    High_yr_schl_comp_Yr_10_eq_P	INT,
    High_yr_schl_comp_Yr_9_eq_M	INT,
    High_yr_schl_comp_Yr_9_eq_F	INT,
    High_yr_schl_comp_Yr_9_eq_P	INT,
    High_yr_schl_comp_Yr_8_belw_M	INT,
    High_yr_schl_comp_Yr_8_belw_F	INT,
    High_yr_schl_comp_Yr_8_belw_P	INT,
    High_yr_schl_comp_D_n_g_sch_M	INT,
    High_yr_schl_comp_D_n_g_sch_F	INT,
    High_yr_schl_comp_D_n_g_sch_P	INT,
    Count_psns_occ_priv_dwgs_M	INT,
    Count_psns_occ_priv_dwgs_F	INT,
    Count_psns_occ_priv_dwgs_P	INT,
    Count_Persons_other_dwgs_M	INT,
    Count_Persons_other_dwgs_F	INT,
    Count_Persons_other_dwgs_P INT
);

CREATE TABLE if not exists RAW.census_data_g02 (
    LGA_CODE VARCHAR(255) PRIMARY KEY,
    Median_age_persons INT,
    Median_mortgage_repay_monthly INT,
    Median_tot_prsnl_inc_weekly INT,
    Median_rent_weekly INT,
    Median_tot_fam_inc_weekly INT,
    Average_num_psns_per_bedroom FLOAT,
    Median_tot_hhd_inc_weekly INT,
    Average_household_size FLOAT
);

CREATE TABLE IF NOT EXISTS RAW.listings (
    LISTING_ID NUMERIC,
    SCRAPE_ID NUMERIC,
    SCRAPED_DATE DATE,
    HOST_ID NUMERIC,
    HOST_NAME VARCHAR(255),
    HOST_SINCE VARCHAR(255),
    HOST_IS_SUPERHOST VARCHAR(255),
    HOST_NEIGHBOURHOOD VARCHAR(255),
    LISTING_NEIGHBOURHOOD VARCHAR(255),
    PROPERTY_TYPE VARCHAR(255),
    ROOM_TYPE VARCHAR(255),
    ACCOMMODATES NUMERIC,
    PRICE FLOAT,
    HAS_AVAILABILITY VARCHAR(255),
    AVAILABILITY_30 NUMERIC,
    NUMBER_OF_REVIEWS NUMERIC,
    REVIEW_SCORES_RATING NUMERIC,
    REVIEW_SCORES_ACCURACY NUMERIC,
    REVIEW_SCORES_CLEANLINESS NUMERIC,
    REVIEW_SCORES_CHECKIN NUMERIC,
    REVIEW_SCORES_COMMUNICATION NUMERIC,
    REVIEW_SCORES_VALUE NUMERIC
);

CREATE TABLE IF NOT EXISTS RAW.lga_code_name (
	LGA_CODE INT PRIMARY KEY,
	LGA_NAME VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS RAW.lga_name_suburb (
	LGA_NAME VARCHAR(255),
	LGA_SUBURB VARCHAR(255) PRIMARY KEY
);

---------------------------
-- FUNCTIONS
---------------------------

CREATE OR REPLACE FUNCTION safe_date(input_date TEXT, format TEXT)
	RETURNS DATE AS $$
		BEGIN
    		RETURN TO_DATE(input_date, format);
		EXCEPTION WHEN OTHERS THEN
    		RETURN NULL;
	END;
$$ LANGUAGE plpgsql;

