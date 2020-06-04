
### generating matching table


import pandas as pd
import json 

def mapping(f_content, idx):
    
    f_content2 = f_content[f_content.index(idx):]
    f_content2 = f_content2[:f_content2.index(';')].split('\n')
    f_content2 = [i.replace("'", "") for i in f_content2]
    dic = [i.split('=') for i in f_content2[1:]]
    dic = dict([i[0].strip(), i[1].strip()] for i in dic if len(i) == 2)
    return dic

mapping_dict = {}
with open('./I94_SAS_Labels_Descriptions.SAS') as f:
    label_desc = f.read()
    label_desc = label_desc.replace('\t', '')
    i94cntyl = mapping(label_desc, "i94cntyl")    
    i94prtl = mapping(label_desc, "i94prtl")
    i94model = mapping(label_desc, "i94model")
    i94addrl = mapping(label_desc, "i94addrl")
    i94visa = {'1':'Business',
    '2': 'Pleasure',
    '3' : 'Student'}
    
    mapping_dict.update({"country_mapping": i94cntyl})
    mapping_dict.update({"portOfEntry_mapping": i94prtl})
    mapping_dict.update({"modeOfEntry_mapping": i94model})
    mapping_dict.update({"stateOfAddr_mapping": i94addrl})
    mapping_dict.update({"visaCat_mapping": i94visa})

with open('staging/immigration_mapping.json', 'w') as fp:
    json.dump(mapping_dict, fp, sort_keys=True, indent=4)    