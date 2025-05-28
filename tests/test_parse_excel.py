import os
import io
import warnings
from model.study_file import StudyFile
from d4kms_generic import ServiceEnvironment
import traceback

def api_v1():
    warnings.warn(UserWarning("api v1, should use functions from v2"))
    return 1

se = ServiceEnvironment()

def excel_to_bytes(file_path):
    with open(file_path, 'rb') as file:
        excel_bytes = file.read()
    return excel_bytes

def test_parse_excel():
    try:
        sf = StudyFile()

        # CDISC Pilot
        filename='/Users/johannes/Library/CloudStorage/OneDrive-data4knowledge/shared_mac/usdm/data/CDISC_Pilot_Study_Exposure.xlsx'

        print("importing ", filename)
        contents = excel_to_bytes(filename)
        success = sf.create(filename, contents)
        sf.execute()
    except Exception as e:
        print(f"Exception {e}\n{traceback.format_exc()}")
    print("---- imported ", filename)

if __name__ == "__main__":
    test_parse_excel()
