"""Functions specific to data from Snohomish County HMIS extraction."""
import numpy as np
import os.path as op
import pandas as pd
import puget_utils as pu
import tools
# The default data directory will be '../../data'
# This can be redefined by the user of the function.
data_dir = op.join('..', '..', 'data')

def get_snohomish_data():
    ''' Reads in the Snohomish .csv file.

        Parameters
        ----------
        none
        ----------

        Returns
        ----------
        dataframe with complete Snohomish data
    '''
    # Read the Snohomish file
    data_dir = op.join('..', '..', 'data', 'snohomish')
    filename = op.join(data_dir,'Snohomish_HMIS_Data_Export_06152015.xlsx')
    xl = pd.ExcelFile(filename)
    xl.sheet_names
    df = xl.parse('Query2')
    return df

def get_snohomish_families():
    ''' Reads in the Snohomish data, and keeps only rows that are families

        Parameters
        ----------
        none

        Returns
        ----------
        dataframe with only clients that come from a household with multiple people (a family)
    '''
    df = get_snohomish_data()

    gb = df.groupby('FamilyID')
    more_than_one = lambda x:(len(np.unique(x['ClientID'])) > 1)
    families = gb.filter(more_than_one).copy()
    #returns dataframe with families with more than one person (may be a family of all children or all adults)

    #Remove data entry errors:
    for famid, fam in families.groupby('FamilyID'):
        for caseid, case in fam.groupby('CaseID'):
            if not caseid == fam.iloc[0]['CaseID'] :  #if not the same caseID as was identified in the fam table
                if case['ClientID'].nunique() == 1 : # case['EnrollDate'].iloc[0] == prv_case_EnrollDate) :
                #if this condition is true, this case is an error
                    families = families[families.CaseID != caseid]
                else:
                    pass

            else: #if it is the same caseID as was identified in the fam table
                pass


    families = recode_snohomish_data(families)

    return families


def recode_snohomish_data(families):
    ''' Recodes variables that had errors or needed aggregating to create episodes table.

        Parameters
        ----------
        families: a dataframe generated from the get_snohomish_families function
        ----------

        Returns
        ----------
        Dataframe with recoded variables.
    '''

    #Rename Housing with Services and RETIRED values
    families['ProgramType'] = np.where(families['ProgramType']=='PH - Housing With Services (disability required for entry)',
                                       'PH - Housing With Services (no disability required for entry)',
                                       np.where(families['ProgramType']=='RETIRED', 'Legacy HPRP', families['ProgramType']))



    #Rename DestinationAtExit to something else due to conflicts with viztric
    families = families.rename(columns={'DestinationAtExit': 'Dest'})

    #Recode DestinationAtExit to numeric  before running the viztric

    families['DestinationAtExit_Numeric'] = families['Dest'].map({'Rental by client, no ongoing housing subsidy': 10,
       'Rental by client, other (non-VASH) ongoing housing subsidy': 20,
       'Transitional Housing for homeless persons (including homeless youth)':2, "Don't Know": 8,
       'Permanent housing for formerly homeless persons (such as: CoC project; or HUD legacy programs; or HOPWA PH)': 3,
       'Staying or living with family, temporary tenure (e.g., room, apartment or house)' : 12,
       'Staying or living with family, permanent tenure': 22, 'Rental by client, VASH Subsidy': 19,
       'Staying or living with friends, temporary tenure (e.g., room, apartment or house)': 13,
       'Emergency Shelter, including hotel or motel paid for with shelter voucher': 1, 'Refused': 9,
       'Place not meant for habitation (e.g., a vehicle, an abandoned building, bus/train/subway station/airport or anywhere outside)': 16,
       'Staying or living with friends, permanent tenure': 23, 'Substance Abuse Treatment or Detox Center': 5,
       'Hotel or Motel paid for without Emergency Shelter Voucher': 14, 'Other': 17, 'Jail, Prison, Juvenile Detention Facility' : 7,
       'Foster Care Home or Foster Care Group Home': 15, 'Owned by client, no ongoing housing subsidy': 11,
       'No exit interview complete':30, 'Safe Haven':18, 'Hospital or other residential non-psychiatric medical facility':6,
       'Psychiatric Hospital or Other Psychiatric Facility':4, 'Deceased':24, 'Data not collected': 99})

     #merge in viztric variables. This is necessary for get_data pipeline
    families = tools.merge_viztric_destination(data=families, destination_colname='DestinationAtExit_Numeric',
                viztric_map_fname='viztric_destination_mappings.csv',
                directory = op.join('..','..'), destination_outcome_type='Value')

    #add new county variable to family dataframe
    families['County'] = "Snohomish"

    # recode RelationshipHoH variable
    families['RelationshipHoH'] = families['RelationshipHoH'].map({'Self': 1, 'Son': 2, 'Daughter':2,
                                                              'Spouse/Partner':3, 'Dependent Child':2,
                                                               'Other Family Member':4, 'Other Non-Family':5,
                                                               'Parent':4, 'Other Caretaker': 5, 'Grandparent':4,
                                                               'Guardian':5})

    #recode Gender
    families['Gender'] = families['Gender'].map({'Female': 0, 'Male': 1, 'Client refused': np.NaN,
                                             "Client doesn't know" : np.NaN,
                                             'Transgender female to male' : 1})
    #recode Race
    families['Race'] = np.where(families['Race']=='Client refused', np.NaN, np.where(families['Race']=="Client doesn't know",
                            np.NaN, np.where(families['Race']=='Data not collected', np.NaN, families['Race'])))

    #create new length of stay variable
    families['LengthOfStay']=families.apply(lambda row: row['ExitDate']-row['EnrollDate'], axis=1)

    #create new age at entry variable using year of birth and year of entry
    families['AgeAtEntry'] = families.apply(lambda row: row['EnrollDate'].year - row['BirthDate'].year, axis=1)

    #convert negative ages to NaN (these were people born during enrollment)
    families['AgeAtEntry'] = np.where(families['AgeAtEntry']<0, 0, families['AgeAtEntry'])

    #drop observations with birthdate in 1900
    families = families[families.BirthDate.dt.year != 1900]

    #DISABILITIES
    #Devdis and phys has 2
    #disabling condition has 9 and 8
    #Chronic and HIV/AIDS and Mental is correct
    #Substance abuse has 0,1,2, which is correct
    for i in range(families.shape[0]) :
        if families['DevDis'].iloc[i] == 2:
            families['DevDis'].iloc[i] = np.nan
        else:
            pass
        if families['PhysDisability'].iloc[i] ==2 :
            families['PhysDisability'].iloc[i] = np.nan
        else:
            pass
        if (families['DisablingCondition'].iloc[i] == 8 or families['DisablingCondition'].iloc[i] == 9) :
            families['DisablingCondition'].iloc[i] = np.nan
        else:
            pass

    #Create joint disabilities
    families['JointDisabilities'] = families.apply(lambda row:
                                                     max(row['PhysDisability'], row['DevDis'], row['Chronic_Health'],
                                                         row['HIV/AIDS']), axis=1)
    #INSURANCE
    #create binariers for entry and exit insurance variables
    for h in ['DataBookEntry_', 'DataBookExit_'] :
        for d in ["StateChildren'sHealthInsurance", 'Medicaid', 'Medicare', 'unemploymentinsurance', 'PrivateDisabilityInsurance'] :
            families[h + d + "_binary"] = np.where(families[h+d] > 0, 1, families[h+d])


    #Take max of insurance
    #InsuranceFromAnySource
    families['InsuranceFromAnySource_Binary'] = families.apply(lambda row:
                                                     max(row["DataBookEntry_StateChildren'sHealthInsurance_binary"],
                                                         row["DataBookExit_StateChildren'sHealthInsurance_binary"],
                                                         row['DataBookEntry_Medicaid_binary'],
                                                        row['DataBookExit_Medicaid_binary'],
                                                        row['DataBookEntry_Medicare_binary'],
                                                        row['DataBookExit_Medicare_binary'],
                                                        row['DataBookEntry_unemploymentinsurance_binary'],
                                                        row['DataBookExit_unemploymentinsurance_binary'],
                                                        row['DataBookEntry_PrivateDisabilityInsurance_binary'],
                                                        row['DataBookExit_PrivateDisabilityInsurance_binary']), axis=1)


    #Create if ever binaries (max of entry and exit binaries) for Medicaid, Medicare, and StateChildren'sHealthInsurance
    families['MedicaidEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_Medicaid_binary'], row['DataBookExit_Medicaid_binary']), axis=1)

    families['MedicareEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_Medicare_binary'], row['DataBookExit_Medicare_binary']), axis=1)

    families["StateChildren'sHealthInsuranceEver_Binary"] = families.apply(lambda row:
                                                 max(row["DataBookEntry_StateChildren'sHealthInsurance_binary"],
                                                     row["DataBookEntry_StateChildren'sHealthInsurance_binary"]), axis=1)




    #Replace 9 and 8 in VerteranStatus with nans
    for i in range(families.shape[0]) :
        if (families['VeteranStatus'].iloc[i] == 8 or families['VeteranStatus'].iloc[i] == 9):
            families['VeteranStatus'].iloc[i] = np.nan
        else:
            pass


    #Take max of EmployedBinary at entry and exit
    families['EmployedEver_Binary'] = families.apply(lambda row:
                                                     max(row['DatabookEntry_EmployedBinary'], row['DataBookExit_EmployedBinary']), axis=1)


    #income info - turn into binary for entry and exits
    for h in ['DataBookEntry_', 'DataBookExit_'] :
        for d in ['EarnedIncome', 'TANF', 'GeneralAssistance', "Child'sSupport"] :
            families[h + d + "_binary"] = np.where(families[h+d] > 0, 1, families[h+d])


    #take max of all income variables
    families['Income_Binary'] = families.apply(lambda row:
                                                     max(row["DataBookEntry_EarnedIncome_binary"],
                                                         row["DataBookExit_EarnedIncome_binary"],
                                                         row['DataBookEntry_TANF_binary'],
                                                        row['DataBookExit_TANF_binary'],
                                                        row['DataBookEntry_GeneralAssistance_binary'],
                                                        row['DataBookExit_GeneralAssistance_binary'],
                                                        row["DataBookEntry_Child'sSupport_binary"],
                                                        row["DataBookExit_Child'sSupport_binary"]), axis=1)

    #compute income amount changes
    for c in ["EarnedIncome", "TANF", "GeneralAssistance", "Child'sSupport"] :
        families[c + '_Change'] = families.apply(lambda row: row['DataBookExit_' + c] - row['DataBookExit_' + c], axis=1)

    #Compute 'ever' binaries (max of entry and exit binary variables)
    families['EarnedIncomeEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_EarnedIncome_binary'], row['DataBookExit_EarnedIncome_binary']),
                                                         axis=1)

    families['TANFEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_TANF_binary'], row['DataBookExit_TANF_binary']), axis=1)

    families['GeneralAssistanceEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_GeneralAssistance_binary'],
                                                     row['DataBookExit_GeneralAssistance_binary']), axis=1)

    families["Child'sSupportEver_Binary"] = families.apply(lambda row:
                                                 max(row["DataBookEntry_Child'sSupport_binary"],
                                                     row["DataBookExit_Child'sSupport_binary"]), axis=1)



    #Benefits - change to binary for entry and exit
    for h in ['DataBookEntry_', 'DataBookExit_'] :
        for d in ['TANFChildCareServices'] :
            families[h + d + '_binary'] = np.where(families[h+d] > 0, 1, families[h+d])
    #no exit variable for TempRent
    families['DataBookEntry_TempRent_binary'] = np.where(families['DataBookEntry_TempRent'] > 0, 1,
                                                         families['DataBookEntry_TempRent'])

    #take max of benefits
    families['BenefitsFromAnySource_Binary'] = families.apply(lambda row:
                                                     max(row["DataBookEntry_TANFChildCareServices_binary"],
                                                         row["DataBookExit_TANFChildCareServices_binary"],
                                                         row["DataBookEntry_TempRent_binary"]),axis=1)

    #take max of TANFChildCare between entry and exit binaries (if ever)
    families['TANFChildCareEver_Binary'] = families.apply(lambda row:
                                                 max(row['DataBookEntry_TANFChildCareServices_binary'],
                                                     row['DataBookExit_TANFChildCareServices_binary']), axis=1)

    return families
