from model.study import Study
from model.study_design import StudyDesign
from model.study_version import StudyVersion
from model.study_arm import StudyArm
from d4kms_service import Neo4jConnection
from d4kms_generic import application_logger
from d4kms_generic import ServiceEnvironment


def clear_db():
    db = Neo4jConnection()
    with db.session() as session:
        query = """
            match (a:Study)-[]-(b)
            where a.name = "DELETE_ME"
            optional match (b)-[]-(c)
            optional match (c)-[]-(d)
            optional match (d)-[]-(e)
            detach delete a, b, c, d, e
        """
        session.run(query)

        query = """
            match (a:Study)-[]-(b)
            where a.name = "CDISC_Pilot_Study-compressed.pdf"
            optional match (b)-[]-(c)
            optional match (c)-[]-(d)
            optional match (d)-[]-(e)
            detach delete a, b, c, d, e
        """
        session.run(query)
        query = """
            MATCH (n:PdfFile) detach delete n
        """
        session.run(query)
        query = """MATCH (n) where n.delete = "me" detach delete n"""
        session.run(query)
    db.close()
    print("Cleared db")

def main():
    clear_db()
    study = Study.create("DELETE_ME", "test", "test", "test")
    print('study',study)
    sv = StudyVersion.find(study['StudyVersion'])
    print('sv',sv.uuid)


    # studies = Study.list(page = 0, size = 10, filter = "")
    # # for x in studies['items']:
    # #     print(x['name'])
    # cdisc = next((x['uuid'] for x in studies['items'] if x['name'] == "Study_CDISC PILOT - LZZT"), None)
    # print(cdisc)


    # study = Study.find("test")
    # study
    # sd = StudyDesign.list()
    # for x in sd:
    #     print(x)
    # study_arm = StudyArm.list()
    # print(study_arm)

if __name__ == "__main__":
    main()
    # print(StudyArm.find("2c3d3e1f-3d5c-4b9c-8d4c-0d8c2d7b3f1a"))
    print("done")
