import pandas as pd
df = pd.read_csv('data/en_openfoodfacts_org_products.csv', sep='\t')
print(df.keys())
df = df[["code", "product_name", "brands", "categories", "countries",
                         "nutriscore_grade", "nova_group", "pnns_groups_1", "pnns_groups_2", "ecoscore_grade"]]

print(df.keys())
df.to_csv('data/en.openfoodfacts.org.products.csv', sep=',')