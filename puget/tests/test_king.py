import puget.king as pk
import puget
import os.path as op

DATA_PATH = op.join(puget.__path__[0], 'data', 'king')

def test_read_table():
    df = pk.read_table('Enrollment.csv', DATA_PATH, years=2011,
                        columns_to_drop=['OtherResidencePrior','StatusDocumented',
                     'HousingStatus','DateOfEngagement',
                   'InPermanentHousing', 'ResidentialMoveInDate', 'DateOfPATHStatus',
                   'ClientEnrolledInPATH', 'ReasonNotEnrolled', 'WorstHousingSituation', 'PercentAMI',
                   'AddressDataQuality', 'DateOfBCPStatus', 'FYSBYouth', 'ReasonNoServices', 'SexualOrientation',
                   'FormerWardChildWelfare', 'ChildWelfareYears', 'ChildWelfareMonths', 'FormerWardJuvenileJustice',
                   'JuvenileJusticeYears', 'JuvenileJusticeMonths', 'HouseholdDynamics',
                   'SexualOrientationGenderIDYouth', 'SexualOrientationGenderIDFam', 'HousingIssuesYouth',
                   'HousingIssuesFam', 'SchoolEducationalIssuesYouth', 'SchoolEducationalIssuesFam',
                   'UnemploymentYouth', 'UnemploymentFam', 'MentalHealthIssuesYouth', 'MentalHealthIssuesFam',
                   'HealthIssuesYouth', 'HealthIssuesFam', 'PhysicalDisabilityYouth', 'PhysicalDisabilityFam',
                   'MentalDisabilityYouth', 'MentalDisabilityFam', 'AbuseAndNeglectYouth', 'AbuseAndNeglectFam',
                   'AlcoholDrugAbuseYouth', 'AlcoholDrugAbuseFam', 'InsufficientIncome', 'ActiveMilitaryParent',
                   'IncarceratedParent', 'IncarceratedParentStatus', 'ReferralSource',
                   'CountOutreachReferralApproaches', 'ExchangeForSexPastThreeMonths', 'CountOfExchangeForSex',
                   'AskedOrForcedToExchangeForSex',
                   'DateCreated', 'DateUpdated', 'UserID', 'DateDeleted', 'ExportID','CSV_directory'],
                       categorical_var=['ResidencePrior', 'ResidencePriorLengthOfStay','DisablingCondition',
                                        'ContinuouslyHomelessOneYear','TimesHomelessPastThreeYears',
                                        'MonthsHomelessPastThreeYears'],
                       time_var=['EntryDate'])
