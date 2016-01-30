def destination_map(data, map_table = 'destination_mappings.csv'):
        data_path = os.getcwd()
        mapping_table = pd.read_csv(op.join(data_path,'data','metadata', map_table))
        mapping_table = mapping_table[mapping_table.Standard == "New Standards"]
        # Recode Subsidy column to boolean
        mapping_table['Subsidy'] = mapping_table['Subsidy'].map({'Yes': True,
                                                                 'No': False})
        # Drop columns we don't need
        mapping_table = mapping_table.drop(['Standard'], axis=1)

        # Merge the Destination mapping with the df
        # based on the last_destination string
        output_df = pd.merge(left=data, right=mapping_table, how='left',
                         left_on='DestinationAtExit',
                         right_on='DestinationDescription')

        output_df = output_df.drop('DestinationDescription', axis=1)

        return output_df
