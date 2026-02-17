# include function to remove data from any particular province. 

import numpy as np
import pandas as pd
import time
import os
import datetime
import sys

# this could change to wherever the location of the code is
sys.path.append('..')
import compare
import manager_review as mr
from common_request import *

def IA9_FILTER(hc_or_ltcf, df, fiscal_end):
    """
    This function handles filtering by data element 
    IA9 depending on whether it is an HC or LTFC cut. 

    Args:
        hc_or_ltcf: 
            a string, either 'HC' or 'LTCF'
        df:
            the dataframe being filtered
        fiscal_end:
            the fiscal end, in date format
    Returns:
        the fitlered dataframe
    """
    if hc_or_ltcf == 'HC':
        df = df.rename(columns = {'IA9':'iA9'})
        filt = (df['iA9'] != np.nan) & \
                 (df['iA9'] <=  pd.to_datetime(fiscal_end)) & \
                 (df['ORGANIZATION_IDENTIFIER'] != 'Y0000')
    else:
        filt = (df['IA9'] <= pd.to_datetime(fiscal_end))
    return df[filt]

def CLIENT_FILTER(hc_or_ltcf, df1, df2, fiscal_end):
    """
    This function handles filtering the client encounter
    table on whether it is an HC or LTFC cut. 

    Args:
        hc_or_ltcf: 
            a string, either 'HC' or 'LTCF'
        df1:
            the client encounter dataframe
        df2:
            the asssessment dataframe
        fiscal_end:
            the fiscal end, in date format'
    Returns:
        the fitlered df1 dataframe
    """
    if hc_or_ltcf == 'HC':
        new_name = {'encounter_id':'ENCOUNTER_ID'}
        df1 = df1.rename(columns = new_name)
        df2 = df2.rename(columns = new_name)
        filt = (df1['ENCOUNTER_ID'].isin(df2['ENCOUNTER_ID']))
    else:
        filt = ((df1['CIHIB2'].isna()) & \
                 (df1['IB2'] <= fiscal_end)) | \
                 ((df1['IB2'].isna()) & \
                 (df1['CIHIB2'] <= fiscal_end))
    return df1[filt]

def CLIENT_PHI_FILTER(hc_or_ltcf, df):
    """
    This function handles filtering the client phi
    table on whether it is an HC or LTFC cut. 

    Args:
        hc_or_ltcf: 
            a string, either 'HC' or 'LTCF'
        df:
            the client phi dataframe being filteredd

    Returns:
        the fitlered df1 dataframe
    """
    if hc_or_ltcf == 'HC':
        filt = ['ENCOUNTER_ENTRY_ID', 'IA10']
    else:
        filt = ['CLIENT_ENTRY_ID', 'iA3', 'IA10']
    return df[filt]

def data_request(hc_or_ltcf, date):
    """
    The primary process for handling the stats canada data
    request. 

    Args:
        hc_or_ltcf: 
            a string, either 'HC' or 'LTCF'
        data:
            the date of the datacut, for example,
            if year is 2024, and require Q4 data,
            data = 'FY2024Q4'
    Returns:
        the fitlered df1 dataframe
    """
    if hc_or_ltcf == 'HC':
        analytical_path = "~/Data/Groups/IRRS/PHISecure/DATA/Analytical File HC/" + date
        assessment = pd.read_parquet(analytical_path + "/parquet_files/assessment_final_prod.parquet")
        medication = pd.read_parquet(analytical_path + "/parquet_files/medication_final_prod.parquet")
        disease_diagnoses = pd.read_parquet(analytical_path + "/parquet_files/disease_diagnoses_final_prod.parquet")
        client_encounter = pd.read_parquet(analytical_path + "/parquet_files/client_encounter_final_prod.parquet")
        org = pd.read_parquet(analytical_path + "/parquet_files/organization_final.parquet")
        client_phi = pd.read_parquet(analytical_path + "/parquet_files/client_phi_final_prod.parquet")
    
        client_merge = ['ENCOUNTER_ENTRY_ID']
        provinces = ['ON', 'PE']
        method = 'include'
    else:
        analytical_path = "~/Data/Groups/IRRS/PHISecure/DATA/Analytical File/" + date[0:8]
        assessment = pd.read_parquet(analytical_path + "/Parquet_files/assessment_ltcf.parquet")
        medication = pd.read_parquet(analytical_path + "/Parquet_files/medication.parquet")
        disease_diagnoses = pd.read_parquet(analytical_path + "/Parquet_files/disease_diagnoses.parquet")
        client_encounter = pd.read_parquet(analytical_path + "/Parquet_files/client_encounter.parquet")
        ad1_privatepay = pd.read_parquet(analytical_path + "/Parquet_files/ad1_privatepay.parquet")
        ad3_misfunctionalcentre = pd.read_parquet(analytical_path + "/Parquet_files/ad3_misfunctionalcentre.parquet")
        ad4_programtype = pd.read_parquet(analytical_path + "/Parquet_files/ad4_programtype.parquet")
        org = pd.read_parquet(analytical_path + "/Parquet_files/winston_test/org.parquet")
        client_phi = pd.read_sas(analytical_path + "/PHI/client_phi.sas7bdat", encoding = 'utf-8')
    
        client_merge = ['CLIENT_ENTRY_ID']
        provinces = ['CA']
        method = 'exclude'
    
    fiscal_end = datetime.date(int(date[2:6])+1, 3, 31) 
    assessment['IA9'] = pd.to_datetime(assessment['IA9'])
    assessment = IA9_FILTER(hc_or_ltcf = hc_or_ltcf,
                            df = assessment,
                            fiscal_end = fiscal_end
                           )
    
    medication = medication[medication['ASSESSMENT_ENTRY_ID'] \
                 .isin(assessment['ASSESSMENT_ENTRY_ID'])]
    
    disease_diagnoses = disease_diagnoses[disease_diagnoses['ASSESSMENT_ENTRY_ID'] \
                        .isin(assessment['ASSESSMENT_ENTRY_ID'])]
    
    client_encounter = CLIENT_FILTER(hc_or_ltcf = hc_or_ltcf,
                                     df1 = client_encounter,
                                     df2 = assessment,
                                     fiscal_end = fiscal_end
                                    )

    client_phi = CLIENT_PHI_FILTER(hc_or_ltcf = hc_or_ltcf,
                                   df = client_phi
                                  )
    client_encounter_phi = client_encounter.merge(client_phi, on = client_merge, how = 'left')
    
    if hc_or_ltcf != 'HC':
        ad1_privatepay = ad1_privatepay[ad1_privatepay['ENCOUNTER_ENTRY_ID'] \
                         .isin(client_encounter['ENCOUNTER_ENTRY_ID'])]
        ad3_misfunctionalcentre = ad3_misfunctionalcentre[ad3_misfunctionalcentre['ENCOUNTER_ENTRY_ID'] \
                                  .isin(client_encounter['ENCOUNTER_ENTRY_ID'])]
        ad4_programtype = ad4_programtype[ad4_programtype['ENCOUNTER_ENTRY_ID'] \
                          .isin(client_encounter['ENCOUNTER_ENTRY_ID'])]
    
    top_maid = MAID_TOP(df = disease_diagnoses) 
    org = province_handling(df = org,
                            provinces = provinces,
                            method = method
                          )
    main_directory = f"{date[0:6]}-{hc_or_ltcf} StatsCan Annual Data Cut"
    output_directory = main_directory + "/output_tables"
    verify_directory = main_directory + "/verification"
    try:
        os.mkdir(main_directory)
        os.mkdir(output_directory)
        os.mkdir(verify_directory)
    except:
        print("Directories already made. Outputs will be overwritten.")
    else:
        None
        
    # verification
    # needs to be edited later for HC and LTCF

    dfs = [
        assessment, org,
        top_maid, client_encounter
    ]
    df_names = [
        'assessment', 'organization',
        'top_maid', 'client_encounter'
    ]
    df_tups = list(zip(dfs, df_names))

    for i, j in df_tups:
        verification(df = i,
                     df_name = j,
                     hc_or_ltcf = hc_or_ltcf,
                     verify_directory = verify_directory
                    )
    
    if hc_or_ltcf == 'HC':
        HC_med_keep = [
            "Assessment_entry_id",
            "iM1b1",
            "iM1c1",
            "iM1d1",
            "iM1e1",
            "iM1f1",
            "iM1gc1"
        ]
        HC_med_keep = [i.upper() for i in HC_med_keep]
        medication = medication[HC_med_keep]  # unique to hc
        
        HC_org_keep = [
            'Organization_identifier',
            'ORG_NAME_E',
            'ORG_NAME_F',
            'POSTAL_CODE',
            'PROVINCE_CODE',
            'MUNICIPALITY',
            'ORG_REGION_E_DESC',
            'ORG_REGION_F_DESC',
            'ORG_CD_E_DESC',
            'ORG_CMAname',
            'ORG_CSDname',
            'ORG_Urban_Rural_Remote',
            'ORG_Income_Quintile'
        ]
        HC_org_keep = [i.upper() for i in HC_org_keep]
        org = org[HC_org_keep]  # unique entries to hc
        
        HS_disease_keep = [
            'ASSESSMENT_ENTRY_ID',
            'II2AA',
            'II2ABB'    
        ]
        disease_diagnoses = disease_diagnoses[HS_disease_keep] # unique to hc
        
        HC_client_keep = [
            "Organization_identifier", "client_id", "encounter_entry_id", "encounter_id",
            "iA4", "iA5a_encrypt", "iA6d", "iA5d", "iA7a", "iA7b", "iA7d", "iA7e",
            "iA7f", "iA7i", "iA7j", "iA7l", "iA7k", "iA7n", "iA7m", "iB2", "iB4",
            "iT1", "iT2", "cihiA2a", "cihiB2", "age_admission", "age_discharge",
            "age_return", "DQ_Flag_Patient_PostalCode", "DQ_Flag_admission_age",
            "DQ_Flag_discharge_age", "DQ_Flag_return_age", "Fiscal_quarter_discharge",
            "Fiscal_quarter_entry", "Fiscal_quarter_return", "Fiscal_year_discharge",
            "Fiscal_year_entry", "Fiscal_year_return", "PATIENT_CD_CODE",
            "PATIENT_CD_E_DESC", "PATIENT_CIHI_REGION_CODE", "PATIENT_CIHI_REGION_E_DESC",
            "PATIENT_CIHI_REGION_F_DESC", "PATIENT_CMA", "PATIENT_CMAname",
            "PATIENT_CSDname", "PATIENT_CSDtype", "PATIENT_CSDuid", "PATIENT_CSize",
            "PATIENT_CSizeMIZ", "PATIENT_Income_Quintile", "PATIENT_PROVINCE_ABREV_E_DESC",
            "PATIENT_PROVINCE_ABREV_F_DESC", "PATIENT_PROVINCE_CODE", "PATIENT_PROVINCE_E_DESC",
            "PATIENT_PROVINCE_F_DESC", "PATIENT_REGION_CODE", "PATIENT_REGION_E_DESC",
            "PATIENT_REGION_F_DESC", "PATIENT_SACcode", "PATIENT_SACtype",
            "PATIENT_STC_PROVINCE_CODE", "PATIENT_SUBREGION_CODE", "PATIENT_SUBREGION_E_DESC",
            "PATIENT_SUBREGION_F_DESC", "PATIENT_Urban_Rural_Remote"
        ] # FSA is missing
        HC_client_keep = [i.upper() for i in HC_client_keep]   
        client_encounter = client_encounter[HC_client_keep]
        
        HC_ass_keep = [
            "Organization_identifier", "Assessment_entry_id", "Assessment_id", "client_id",
            "encounter_Id", "iA8", "iA9", "iA11b", "iA13", "iA38a", "caA38c",
            "iA12a", "iA12b", "iA12c", "iA2", "iB3h", "iB3i", "iB3j", "iB5a",
            "iB5b", "iB5e", "iB5c", "iB5d", "caB5h", "iC1", "iC4", "iC5",
            "iC2a", "iC2b", "iC2c", "iC3a", "iC3b", "iC3c", "iD1", "iD2",
            "iD3a", "iD4a", "iE1a", "iE1b", "iE1c", "iE1d", "iE1e", "iE1f",
            "iE1g", "iE1h", "iE1i", "iE1j", "iE1k", "iE2a", "iE2b", "iE2c",
            "iE3a", "iE3b", "iE3c", "iE3d", "iE3f", "iE3e", "iF1d", "iF2",
            "iF3", "iF4", "iF1a", "iF1b", "iF1c", "iF1e", "iF1f", "iF1g",
            "iG8a2", "iG1ab", "iG1aa", "iG1bb", "iG1ba", "iG1cb", "iG1ca",
            "iG1db", "iG1da", "iG1eb", "iG1ea", "iG1fb", "iG1fa", "iG1gb",
            "iG1ga", "iG1hb", "iG1ha", "iG2a", "iG2b", "iG2c", "iG2d",
            "iG2e", "iG2f", "iG2g", "iG2h", "iG2i", "iG2j", "iG3", "iG12",
            "iG4", "iG5", "iG6a", "iG6b", "iG7a", "iG7b", "iG9a", "iG9b",
            "iH1", "iH2", "iH3", "iH4", "iI1a", "iI1b", "iI1c", "iI1d",
            "iI1e", "iI1f", "iI1g", "iI1h", "iI1i", "iI1j", "iI1k", "iI1m",
            "iI1l", "iI1n", "iI1w", "iI1o", "iI1p", "iI1q", "iI1r", "iI1s",
            "iI1t", "iJ3", "iJ4", "iJ7", "iJ1g", "iJ1h", "iJ1i", "iJ2a",
            "iJ2b", "iJ2c", "iJ2d", "iJ2e", "iJ2f", "iJ2g", "iJ2h", "iJ2i",
            "iJ2j", "iJ2m", "iJ2k", "iJ2l", "iJ2n", "iJ2o", "iJ2p", "iJ2t",
            "iJ2q", "iJ2r", "iJ2mm", "iJ2s", "iJ5a", "iJ5b", "iJ5c", "iJ5d",
            "iJ5e", "iJ6a", "iJ6b", "iJ6c", "iJ8a", "iJ8b", "iK3", "iK1ab",
            "iK1bb", "iK2a", "iK2c", "iK2b", "iK2h", "iK2g", "iK2e", "iK4a",
            "iK4b", "iK4d", "iK4c", "iL1", "iL2", "iL3", "iL4", "iL5", "iL6",
            "iL7", "iM2", "iM3", "iM14", "iM15", "iM16", "iM17", "iM20", "iM19a",
            "iM19b", "iM19c", "iM19d", "iM22", "iM23", "iN1d", "iN1h", "iN1e",
            "iN1g", "iN1f", "iN1a", "iN1c", "iN1b", "iN2a", "iN2b", "iN2c",
            "iN2d", "iN2e", "iN2f", "iN2g", "iN2h", "iN2i", "iN2j", "iN2k",
            "iN2l", "iN2m", "iN2n", "iN3aa", "iN3ab", "iN3ba", "iN3bb",
            "iN3ca", "iN3cb", "iN3da", "iN3ea", "iN3eb", "iN3fa", "iN3fb",
            "iN3ga", "iN3gb", "iN3ha", "iN3hb", "iN4", "iN5a", "iN5b", "iN5c",
            "caO1i", "caO1j", "iP3", "iF8a", "iP1a1", "iP1a2", "iP1b1", "iP1b2",
            "iP1c1", "iP1c2", "iP1d1", "iP1d2", "iP2a", "iP2b", "iF7d",
            "iQ2", "iQ4", "iQ1a", "iQ1b", "iQ1c", "iQ1d", "iQ1e", "iQ3a",
            "iQ3b", "iQ3c", "iR1", "iR2", "iR3", "iR4", "iR5", "iU2",
            "sADLH", "sADLLF", "sADLSF", "sFUNH", "sABS", "sCHESS", "sCPS",
            "sCPS2", "sCOMM", "sCRISIS", "sDbSI", "sDRS", "sDIVERT", "sIADLCH",
            "aMAPLe", "sPAIN", "aPS", "sPURS", "sVPR", "aCaRE", "cPACTIV",
            "cIADL", "cADL", "cENVIR", "cRISK", "cCOGNIT", "cDELIR", "cCOMMUN",
            "cMOOD", "cBEHAV", "cABUSE", "cBRITSU", "cSOCFUNC", "cFALLS", "cPAIN",
            "cPULCER", "cCARDIO", "cNUTR", "cDEHYD", "cFEEDTB", "cPREVEN", "cDRUG",
            "cADD", "cURIN", "cBOWEL", "aR3H", "caNN9", "sBMI", "DQ_flag_assessment_age",
            "BIRTHYEAR", "age_assessment", "Fiscal_quarter_ax", "Fiscal_year_ax",
            "quarter_ind", "AX_annual_facility_ind", "instrument_type_id"
        ]
        HC_ass_keep = [i.upper() for i in HC_ass_keep if i != 'iA9'] + ['iA9']  
        assessment = assessment[HC_ass_keep]
    else:
        org_keep = [
            "PROVINCE_CODE", "ORGANIZATION_TYPE_CODE", "ORGANIZATION_CODE",
            "ORGANIZATION_NAME_E", "ORGANIZATION_NAME_F", "CORPORATION_CODE",
            "CORPORATION_NAME_E", "CORPORATION_NAME_F", "NUM_OF_BEDS", "FACILITY_SIZE",
            "CITY", "POSTAL_CODE", "ORG_REGION_CODE", "ORG_REGION_E_DESC",
            "ORG_REGION_F_DESC", "ORG_CIHI_REGION_CODE", "ORG_CIHI_REGION_E_DESC",
            "ORG_CIHI_REGION_F_DESC", "ORG_URBAN_RURAL_REMOTE", "ORG_NAME_PLLDJ_E",
            "ORG_NAME_PLLDJ_F", "REGION_CODE", "REGION_E_DESC", "REGION_F_DESC",
        ]
        org = org[org_keep]
        
        client_keep = [
            "PROVINCE_CODE", "CLIENT_ENTRY_ID", "CLIENT_ID", "iA6a", 
            "iA5d", "cihiA2a", "iA5a_encrypt", "iA6d", "iA10", "iA3",
            "iA4", "iA7a", "iA7b", "iA7d", "iA7e", "iA7f", "iA7i", "iA7j", "iA7k",
            "iA7l", "iA7m", "iA7n", "ENCOUNTER_ENTRY_ID", "ENCOUNTER_ID", "iB2", "cihiB2",
            "iT1", "iB6", "iB3h", "iB3i", "iB3j", "iB4", "language_group", "iA11a",
            "iA11b", "caA11c", "iB5a", "iB5b", "iB5c", "iB5d", "iB5e", "caB5h", "iB7",
            "iB11", "iT2", "iT4", "iT5", "iT9", "caT16", "cihiR5", "cihiAD1_admission",
            "cihiAD1_return", "cihiAD1_discharge", "cihiAD2_admission", "cihiAD2_return",
            "cihiAD2_discharge", "cihiAD3_admission", "cihiAD3_return", "cihiAD3_discharge",
            "cihiAD4_admission", "cihiAD4_return", "cihiAD4_discharge", "fiscal_year_entry",
            "fiscal_quarter_entry", "fiscal_year_return", "fiscal_quarter_return", "fiscal_year_discharge",
            "fiscal_quarter_discharge", "PATIENT_CD_CODE", "PATIENT_CD_E_DESC", "PATIENT_CIHI_REGION_CODE",
            "PATIENT_CIHI_REGION_E_DESC","PATIENT_CIHI_REGION_F_DESC", "PATIENT_CMA", "PATIENT_CMAname",
            "PATIENT_CSDname", "PATIENT_CSDtype", "PATIENT_CSDuid", "PATIENT_CSize",
            "PATIENT_CSizeMIZ", "PATIENT_Income_Quintile", "PATIENT_PROVINCE_ABREV_E_DESC",
            "PATIENT_PROVINCE_ABREV_F_DESC", "PATIENT_PROVINCE_CODE", "PATIENT_PROVINCE_E_DESC",
            "PATIENT_PROVINCE_F_DESC", "PATIENT_STC_PROVINCE_CODE", "PATIENT_REGION_CODE",
            "PATIENT_REGION_E_DESC", "PATIENT_REGION_F_DESC", "PATIENT_SACcode", "PATIENT_SACtype",
            "PATIENT_SUBREGION_CODE", "PATIENT_SUBREGION_E_DESC", "PATIENT_SUBREGION_F_DESC",
            "PATIENT_urban_rural_remote", "age_admission", "age_return", "age_discharge"
        ]
        client_keep = [i.upper() for i in client_keep if i != 'iA3'] + ['iA3']
        client_encounter_phi = client_encounter_phi[client_keep]
    
    # final outputs 
    assessment.to_csv(output_directory + "/assessment.csv", index = False)
    client_encounter_phi.to_csv(output_directory + "/client_encounter.csv", index = False)
    disease_diagnoses.to_csv(output_directory + "/disease_diagnoses.csv", index = False)
    medication.to_csv(output_directory + "/medication.csv", index = False)
    org.to_csv(output_directory + "/org.csv", index = False)

    if hc_or_ltcf == 'LTCF':
        ad1_privatepay.to_csv(output_directory + "/ad1_privatepay.csv", index = False)
        ad3_misfunctionalcentre.to_csv(output_directory + "/ad3_misfunctionalcentre.csv", index = False)
        ad4_programtype.to_csv(output_directory + "/ad4_programtype.csv", index = False)