from model.study import Study
from model.study_design import StudyDesign
from model.study_version import StudyVersion
from model.study_arm import StudyArm
from model.study_element import StudyElement
from model.study_cell import StudyCell
from model.study_epoch import StudyEpoch
from model.scheduled_instance import ScheduledActivityInstance
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

        query = """MATCH (n) where n.name = "DELETE_ME" detach delete n"""
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

    study_element_uuid = StudyElement.create("DELETE_ME", "test", "test")
    print('study_element_uuid', study_element_uuid)
    study_element = StudyElement.find(study_element_uuid)
    print('study_element', study_element)
    print('study_element.__class__', study_element.__class__)

    study_cell_uuid = StudyCell.create()
    print('study_cell_uuid', study_cell_uuid)
    study_cell = StudyCell.find(study_cell_uuid)
    print('study_cell', study_cell)
    print('study_cell.__class__', study_cell.__class__)

    study_epoch_uuid = StudyEpoch.create("DELETE_ME", "test", "test")
    print('study_epoch_uuid', study_epoch_uuid)
    study_epoch = StudyEpoch.find(study_epoch_uuid)
    print('study_epoch', study_epoch)
    print('study_epoch.__class__', study_epoch.__class__)

    study_epoch_uuid = StudyEpoch.create("DELETE_ME", "test", "test")
    print('study_epoch_uuid', study_epoch_uuid)
    study_epoch = StudyEpoch.find(study_epoch_uuid)
    print('study_epoch', study_epoch)
    print('study_epoch.__class__', study_epoch.__class__)

    sai_uuid = ScheduledActivityInstance.create("DELETE_ME", "test", "test")
    print('sai_uuid', sai_uuid)
    scheduled_activity_instance = ScheduledActivityInstance.find(sai_uuid)
    print('scheduled_activity_instance.__class__', scheduled_activity_instance.__class__)
    print('scheduled_activity_instance', scheduled_activity_instance)



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
