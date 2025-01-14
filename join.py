import pandas as pd

# Load the CSV files into DataFrames
counts_df = pd.read_csv('all_counts_5_9.csv')
merch_items_df = pd.read_csv('all_merch_items_5_9.csv')
shows_df = pd.read_csv('all_shows_5_9.csv')

# Join counts_df with merch_items_df on 'merchVariantUuid' and 'variantUuid'
merged_df_1 = pd.merge(counts_df, merch_items_df, left_on='merchVariantUuid', right_on='variantUuid')

# Join the result with shows_df on 'show_uuid'
final_merged_df = pd.merge(merged_df_1, shows_df, on='show_uuid')

# Save the final merged DataFrame to a new CSV file
final_merged_df.to_csv('joined_data_5_9.csv', index=False)