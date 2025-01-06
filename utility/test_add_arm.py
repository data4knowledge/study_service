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
        # query = """
        #     match (a:Study)-[]-(b)
        #     where a.name = "CDISC_Pilot_Study-compressed.pdf"
        #     optional match (b)-[]-(c)
        #     optional match (c)-[]-(d)
        #     optional match (d)-[]-(e)
        #     detach delete a, b, c, d, e
        # """
        # session.run(query)

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
            MATCH (n:PdfFile) detach delete n
        """
        session.run(query)

        query = """
            MATCH (n:StudyFile) where n.filename = "CDISC_Pilot_Study-compressed.pdf" detach delete n
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
    print('sv',sv)

    sd_uuid = StudyDesign.create("DELETE_ME", "test", "test")
    sd = StudyDesign.find(sd_uuid)
    print('sd',sd)

    sv.relationship(sd, "STUDY_DESIGNS_REL")

    study_arm_uuid = StudyArm.create("DELETE_ME", "test", "test")
    print('study_arm_uuid', study_arm_uuid)
    study_arm = StudyArm.find(study_arm_uuid)
    # study_arm = StudyArm.find(study_arm_uuid, raw = True)
    print('study_arm', study_arm)
    print('study_arm.__class__', study_arm.__class__)
    # x = 


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
    print("done")
