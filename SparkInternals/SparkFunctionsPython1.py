# Function to transform

FEATURE_INDEX_MAP = """OUTPUT_OF<DataFrame.schema(DataFrame.schema.fieldIndex("vectorAssemblesOutputColumnName")).metadata>"""
FEATURE_IMPORTANCE_LIST = sparkModel.featureImportance--List


def Method_FeatureImportance(FEATURE_IMPORTANCE_LIST, FEATURE_INDEX_MAP):
    #
    import pandas as pd
    import json
    #
    # Loading FEATURE_INDEX_MAP as a dictionary
    json_dict = json.loads(FEATURE_INDEX_MAP)
    #
    #
    my_dict = {}
    for type_ in ['numeric', 'binary']:
        for i in json_dict['ml_attr']['attrs'][type_]:
            my_dict[i['idx']] = i['name']
    #
    my_dict2 = {}
    for i in range(0, 12): # 42 here is the number of features, make it Dynamic!!
        my_dict2[my_dict[i]] = FEATURE_IMPORTANCE_LIST[i]
    #
    import operator
    sorted_x = sorted(my_dict2.items(), key=operator.itemgetter(1), reverse=True)
    #
    my_list = []
    for i in sorted_x:
        my_list.append(i)
    #
    #
    return pd.DataFrame(my_list)


Execute AS:
Method_FeatureImportance(FEATURE_IMPORTANCE_LIST, FEATURE_INDEX_MAP)

 
#####################################################################################################################################################


Example feature_importance_list:

FEATURE_IMPORTANCE_LIST = [4.978612782210185E-5,0.020008978906247834, 0.07816846664101793, 0.0074614656923388655 , 0.01119303971949936, 
    0.0035458862264644117, 0.004625624268082312, 0.5641731701805802, 7.593386750517974E-4,0.011352885871374407 , 0.036656151718899243 , 0.2587746]

FEATURE_INDEX_MAP = """{"ml_attr":{"attrs":{"numeric": [{"idx":  0, "name": "numericColFeatures_unpost_seq"},
{"idx":1, "name": "numericColFeatures_voucher_line_num"},{"idx": 2, "name": "numericColFeatures_distrib_line_num"},
{"idx":3, "name": "numericColFeatures_monetary_amount"}, {"idx": 4, "name": "numericColFeatures_foreign_amount"},
{"idx":5, "name": "numericColFeatures_merch_amt_bse"},   {"idx": 6, "name": "numericColFeatures_journal_line"},
{"idx":7, "name": "numericColFeatures_tax_cd_vat_pct"},  {"idx": 8, "name": "numericColFeatures_vat_calc_amt_bse"},
{"idx":9, "name": "numericColFeatures_pyear"}], "binary":
[{"idx":10, "name": "stringColFeatures_business_unit_gl_vec_892"},
{ "idx":11, "name": "stringColFeatures_business_unit_gl_vec_608"}]}, "num_attrs":12}}"""



 
Method_FeatureImportance(FEATURE_IMPORTANCE_LIST, FEATURE_INDEX_MAP)
                                             0         1
0            numericColFeatures_tax_cd_vat_pct  0.564173
1   stringColFeatures_business_unit_gl_vec_608  0.258775
2          numericColFeatures_distrib_line_num  0.078168
3   stringColFeatures_business_unit_gl_vec_892  0.036656
4          numericColFeatures_voucher_line_num  0.020009
5                     numericColFeatures_pyear  0.011353
6            numericColFeatures_foreign_amount  0.011193
7           numericColFeatures_monetary_amount  0.007461
8              numericColFeatures_journal_line  0.004626
9             numericColFeatures_merch_amt_bse  0.003546
10         numericColFeatures_vat_calc_amt_bse  0.000759
11               numericColFeatures_unpost_seq  0.000050


