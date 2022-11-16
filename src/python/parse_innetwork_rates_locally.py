'''
    This script is not meant for actual functioning within Snowflake. Its main purpose
    is to develop and test locally
'''
import ijson 
import json 
from zipfile import ZipFile

import pandas as pd
from decimal import *
import functools
import datetime

json_filename = './data/2022_10_01_priority_health_HMO_in-network-rates.zip'

def parse_file_headers(p_datafile: str ):
    print('parsing file header ...')
    header_event_types = ['string']
    headers = {}
    with ZipFile(p_datafile) as zf:
        for file in zf.namelist():
            with zf.open(file) as f:
                parser = ijson.parse(f)
                for prefix, event, value in parser:
                    if len(prefix) < 3:
                        continue

                    elif value == None:
                        continue
                
                    elif '.item' in prefix:
                        continue
                    
                    if event in header_event_types:
                        headers[prefix] = value
    
    return headers

def iterate_childobjecttypes_and_save(p_approx_batch_size: int ,p_datafile: str 
    ,p_innetwork_header_id: str ,p_sub_records: list ,p_record_type: str ) -> int:

    batch_records = []
    total_rec_count = len(p_sub_records)
    
    rec_id_str = p_innetwork_header_id
    rec_id_hash = hash(rec_id_str)
    print(f'Parsing and saving child records [{p_record_type}] {rec_id_str}: len: {total_rec_count} ...')

    for idx ,r in enumerate(p_sub_records):
        curr_rec = {}
        curr_rec['record_num'] = idx
        curr_rec['file'] = p_datafile
        curr_rec['innetwork_rec_key'] = rec_id_str
        curr_rec['innetwork_rec_hash'] = rec_id_hash
        curr_rec[p_record_type] = str(r)
        batch_records.append(curr_rec)

        if len(batch_records) >= p_approx_batch_size:
            df = pd.DataFrame(batch_records)
            batch_records.clear()

    # append leftovers
    if len(batch_records) > 0:
        df = pd.DataFrame(batch_records)
        batch_records.clear()

    return total_rec_count

def parse_breakdown_segments(p_datafile: str):
    print('parsing segments ...')
    iter_idx = 0
    with ZipFile(p_datafile) as zf:
            for file in zf.namelist():
                with zf.open(file) as f:
                    for rec in ijson.items(f, 'in_network.item'):
                        iter_idx += 1
                        innetwork_hdr = {x: rec[x] for x in rec if x not in ['negotiated_rates' ,'bundled_codes' ,'covered_services']}

                        header_id = f'''{innetwork_hdr['negotiation_arrangement']}::{innetwork_hdr['name']}'''
                        innetwork_hdr['header_id'] = header_id.upper().replace(' ','_').replace('\t','_')
        
                        c_nr = iterate_childobjecttypes_and_save(10000 ,'asd' 
                            ,innetwork_hdr['header_id'] ,rec['negotiated_rates'] ,'negotiated_rates')

                        if(iter_idx >= 5):
                            break

                        print(innetwork_hdr)


start = datetime.datetime.now()
print(f'Start time: {start}')

fl_headers = parse_file_headers(json_filename)
parse_breakdown_segments(json_filename)

# parser = ijson.parse(f ,buf_size=bufz_size)
# for prefix, event, value in parser:
#     print(f'{prefix} : {event} : {value}')
            
end = datetime.datetime.now()
print(f'End time: {end}')

elapsed = (end - start)
print(f' Elapsed: {elapsed} ')
print('Finished!!')

#----------
# Parsing file header : Elapsed: 0:08:24.941774
# Parsing all seg headers (not the segment) : Elapsed: 0:07:25.568935 
# Parsing all seg headers (all the segment) : Elapsed: 0:10:29.882289

## ----------------
# {'negotiation_arrangement': 'ffs', 'name': 'Placement of breast localization device(s) (eg, clip, metallic pellet, wire/needle, radioactive seeds), percutaneous; first lesion, including ultrasound guidance', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '19285', 'description': 'Placement of breast localization device(s) (eg, clip, metallic pellet, wire/needle, radioactive seeds), percutaneous; first lesion, including ultrasound guidance', 'header_id': 'FFS::PLACEMENT_OF_BREAST_LOCALIZATION_DEVICE(S)_(EG,_CLIP,_METALLIC_PELLET,_WIRE/NEEDLE,_RADIOACTIVE_SEEDS),_PERCUTANEOUS;_FIRST_LESION,_INCLUDING_ULTRASOUND_GUIDANCE'}
# {'negotiation_arrangement': 'ffs', 'name': 'HIP&FEMUR PROC,EX MAJ JNT,AGE >17 W/O CC', 'billing_code_type': 'MS-DRG', 'billing_code_type_version': '', 'billing_code': '211', 'description': 'HIP&FEMUR PROC,EX MAJ JNT,AGE >17 W/O CC   ', 'header_id': 'FFS::HIP&FEMUR_PROC,EX_MAJ_JNT,AGE_>17_W/O_CC'}
# {'negotiation_arrangement': 'ffs', 'name': 'Shave Dermal Les Scald Etc to 0.5cm', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '11305', 'description': 'Shave Dermal Les Scald Etc to 0.5cm', 'header_id': 'FFS::SHAVE_DERMAL_LES_SCALD_ETC_TO_0.5CM'}
# {'negotiation_arrangement': 'ffs', 'name': 'AV ANASTOM OPEN; UP ARM CEPHALIC VEIN TRNSPSTN', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '36818', 'description': 'AV ANASTOM OPEN; UP ARM CEPHALIC VEIN TRNSPSTN', 'header_id': 'FFS::AV_ANASTOM_OPEN;_UP_ARM_CEPHALIC_VEIN_TRNSPSTN'}
# {'negotiation_arrangement': 'ffs', 'name': 'OTHER MALE REPRODUCTIVE SYSTEM O.R. PROCEDURES FOR MALIGNANCY WITH CC/MCC', 'billing_code_type': 'MS-DRG', 'billing_code_type_version': '', 'billing_code': '715', 'description': 'OTHER MALE REPRODUCTIVE SYSTEM O.R. PROCEDURES FOR MALIGNANCY WITH CC/MCC', 'header_id': 'FFS::OTHER_MALE_REPRODUCTIVE_SYSTEM_O.R._PROCEDURES_FOR_MALIGNANCY_WITH_CC/MCC'}
# {'negotiation_arrangement': 'ffs', 'name': 'Tracheostomy mask, each', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': 'A7525', 'description': 'Tracheostomy mask, each', 'header_id': 'FFS::TRACHEOSTOMY_MASK,_EACH'}
# {'negotiation_arrangement': 'ffs', 'name': 'Anes-clav & scapula; nos', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '00450', 'description': 'Anes-clav & scapula; nos', 'header_id': 'FFS::ANES-CLAV_&_SCAPULA;_NOS'}
# {'negotiation_arrangement': 'ffs', 'name': 'Immunization administration by intramuscular injection of severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2) (Coronavirus disease [COVID-19]) vaccine, mRNA-LNP, spik', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '0001A', 'description': 'Immunization administration by intramuscular injection of severe acute respiratory syndrome coronavirus 2 (SARS-CoV-2) (Coronavirus disease [COVID-19]) vaccine, mRNA-LNP, spik', 'header_id': 'FFS::IMMUNIZATION_ADMINISTRATION_BY_INTRAMUSCULAR_INJECTION_OF_SEVERE_ACUTE_RESPIRATORY_SYNDROME_CORONAVIRUS_2_(SARS-COV-2)_(CORONAVIRUS_DISEASE_[COVID-19])_VACCINE,_MRNA-LNP,_SPIK'}
# {'negotiation_arrangement': 'ffs', 'name': 'Anes-all involv veins up leg incl explor', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '01260', 'description': 'Anes-all involv veins up leg incl explor', 'header_id': 'FFS::ANES-ALL_INVOLV_VEINS_UP_LEG_INCL_EXPLOR'}
# {'negotiation_arrangement': 'ffs', 'name': 'Intravascular doppler veloc/pres derived cor flow reserve meas cor/angio w/pharm induced stress first vessel', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '93571', 'description': 'Intravascular doppler veloc/pres derived cor flow reserve meas cor/angio w/pharm induced stress first vessel', 'header_id': 'FFS::INTRAVASCULAR_DOPPLER_VELOC/PRES_DERIVED_COR_FLOW_RESERVE_MEAS_COR/ANGIO_W/PHARM_INDUCED_STRESS_FIRST_VESSEL'}
# {'negotiation_arrangement': 'ffs', 'name': 'Injection, ado-trastuzumab emtansine, 1 mg (Kadcyla)', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': 'J9354', 'description': 'Injection, ado-trastuzumab emtansine, 1 mg (Kadcyla)', 'header_id': 'FFS::INJECTION,_ADO-TRASTUZUMAB_EMTANSINE,_1_MG_(KADCYLA)'}
# {'negotiation_arrangement': 'ffs', 'name': 'Laryngoplasty, medialization, unilateral', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '31591', 'description': 'Laryngoplasty, medialization, unilateral', 'header_id': 'FFS::LARYNGOPLASTY,_MEDIALIZATION,_UNILATERAL'}
# {'negotiation_arrangement': 'ffs', 'name': 'Sperm Count Huhner Test', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '89300', 'description': 'Sperm Count Huhner Test', 'header_id': 'FFS::SPERM_COUNT_HUHNER_TEST'}
# {'negotiation_arrangement': 'ffs', 'name': 'IADNA S. AUREUS METHICILLIN RESISTANT AMP PRB TQ', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '87641', 'description': 'IADNA S. AUREUS METHICILLIN RESISTANT AMP PRB TQ', 'header_id': 'FFS::IADNA_S._AUREUS_METHICILLIN_RESISTANT_AMP_PRB_TQ'}
# {'negotiation_arrangement': 'ffs', 'name': 'Poliovirus Vaccine, Inactivated, (Ipv), Subq Use', 'billing_code_type': 'CPT', 'billing_code_type_version': '', 'billing_code': '90713', 'description': 'Poliovirus Vaccine, Inactivated, (Ipv), Subq Use', 'header_id': 'FFS::POLIOVIRUS_VACCINE,_INACTIVATED,_(IPV),_SUBQ_USE'}
 